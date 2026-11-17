use std::{
    collections::{HashMap, HashSet},
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use nexo_protocol::{
    AppInfo, Capability, HEARTBEAT_INTERVAL_RANGE_MS, MAX_LINE_BYTES, MAX_MENU_ITEMS,
    MISSED_HEARTBEATS_LIMIT, MenuItem, Message, PROTOCOL_VERSION, ProtocolError, QUIT_TIMEOUT_MS,
    QuitReason, RejectCode, VersionRange, decode, encode,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{mpsc, watch},
};

pub(crate) trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}
pub(crate) type Connection = Box<dyn Stream>;
pub(crate) type Connect =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = io::Result<Connection>> + Send>> + Send + Sync>;
pub(crate) type Job = Box<dyn FnOnce() + Send>;

/// The host's decision for a hub-initiated quit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QuitDecision {
    Accept,
    Refuse(String),
}

/// A single-use response to a hub-initiated quit request.
pub struct QuitResponder {
    id: u64,
    sender: mpsc::UnboundedSender<(u64, QuitDecision, tokio::time::Instant)>,
    live: Arc<AtomicBool>,
}
impl QuitResponder {
    pub fn respond(self, decision: QuitDecision) {
        if self.live.load(Ordering::SeqCst) {
            let _ = self
                .sender
                .send((self.id, decision, tokio::time::Instant::now()));
        }
    }
}

pub(crate) trait Icon: Send {
    fn set_visible(&self, visible: bool) -> Result<(), String>;
}
pub(crate) trait Host: Send + Sync {
    fn dispatch(&self, job: Job) -> Result<(), String>;
    fn create_icon(&self) -> Result<Box<dyn Icon>, String>;
    fn action(&self, item_id: String);
    fn supports_show(&self) -> bool {
        false
    }
    fn supports_quit(&self) -> bool {
        false
    }
    fn show(&self) {}
    fn decide_quit(&self, _reason: QuitReason, _response: QuitResponder) {}
    fn finalize_quit(&self) {}
}

pub(crate) struct Config {
    pub app: AppInfo,
    pub menu: Option<Vec<MenuItem>>,
}

enum Command {
    Menu(Vec<MenuItem>),
}

/// A handle for menu replacement, mediated tray visibility and shutdown.
#[derive(Clone)]
pub struct Handle {
    commands: mpsc::UnboundedSender<Command>,
    shutdown: watch::Sender<bool>,
    done: watch::Receiver<bool>,
    visual: Arc<Visual>,
    has_menu: bool,
}

impl Handle {
    /// Replace the app's complete hub menu. A menu must have been configured at startup.
    pub fn replace_menu(&self, items: Vec<MenuItem>) -> Result<(), &'static str> {
        if !self.has_menu {
            return Err("menu capability was not configured");
        }
        validate_menu(&items)?;
        self.visual.replace_menu(items, &self.commands)
    }
    /// Request personal-icon visibility. The hub's setting wins while connected.
    pub fn set_visible(&self, visible: bool) {
        self.visual.request(visible);
    }
    /// Stop the client when the application is exiting; sends `bye` when possible.
    pub fn shutdown(&self) {
        self.visual.stop();
        self.shutdown.send_replace(true);
    }
    /// Stop the client and wait for its connection and retry work to finish.
    pub async fn shutdown_and_wait(&self) {
        self.shutdown();
        let mut done = self.done.clone();
        while !*done.borrow_and_update() {
            if done.changed().await.is_err() {
                break;
            }
        }
    }
}

pub(crate) fn validate_menu(items: &[MenuItem]) -> Result<(), &'static str> {
    if items.len() > MAX_MENU_ITEMS
        || encode(
            PROTOCOL_VERSION,
            &Message::Menu {
                items: items.to_vec(),
            },
        )
        .is_err()
    {
        return Err("menu exceeds protocol limits");
    }
    Ok(())
}

fn menu_ids(items: &[MenuItem]) -> Vec<String> {
    items.iter().map(|item| item.id.clone()).collect()
}

struct VisualState {
    revision: u64,
    session: u64,
    active: bool,
    standalone: bool,
    initial_pending: bool,
    initial_preference: Option<bool>,
    busy: bool,
    desired: Option<bool>,
    applied: Option<bool>,
    scheduled: Option<u64>,
    menu: Vec<String>,
    icon: Option<Box<dyn Icon>>,
}
struct Visual {
    host: Arc<dyn Host>,
    state: Mutex<VisualState>,
}
impl Visual {
    fn update(self: &Arc<Self>, transition: impl FnOnce(&mut VisualState) -> Option<bool>) {
        let next = {
            let mut state = self.state.lock().unwrap();
            if !state.active {
                return;
            }
            let Some(visible) = transition(&mut state) else {
                return;
            };
            if state.desired != Some(visible) {
                state.desired = Some(visible);
                state.revision += 1;
                state.scheduled = None;
            } else if state.busy || state.scheduled == Some(state.revision) {
                return;
            }
            if !state.busy && state.applied == Some(visible) {
                return;
            }
            state.scheduled = Some(state.revision);
            (state.revision, visible)
        };
        self.schedule(next.0, next.1);
    }
    fn schedule(self: &Arc<Self>, revision: u64, visible: bool) {
        let visual = self.clone();
        if let Err(error) = self.host.dispatch(Box::new(move || {
            let icon = {
                let mut state = visual.state.lock().unwrap();
                if !state.active || state.revision != revision || state.scheduled != Some(revision)
                {
                    return;
                }
                state.scheduled = None;
                if state.busy {
                    return;
                }
                state.busy = true;
                state.icon.take()
            };
            let (icon, applied) = match icon {
                Some(icon) => match icon.set_visible(visible) {
                    Ok(()) => (Some(icon), Some(visible)),
                    Err(error) => {
                        log::error!("tray visibility: {error}");
                        (Some(icon), None)
                    }
                },
                None if visible => match visual.host.create_icon() {
                    Ok(icon) => (Some(icon), Some(true)),
                    Err(error) => {
                        log::error!("tray creation: {error}");
                        (None, None)
                    }
                },
                None => (None, Some(false)),
            };
            let next = {
                let mut state = visual.state.lock().unwrap();
                state.icon = icon;
                state.applied = applied;
                state.busy = false;
                if state.active
                    && state.revision != revision
                    && state.scheduled != Some(state.revision)
                {
                    state.scheduled = Some(state.revision);
                    state.desired.map(|desired| (state.revision, desired))
                } else {
                    None
                }
            };
            if let Some((revision, visible)) = next {
                visual.schedule(revision, visible);
            }
        })) {
            let mut state = self.state.lock().unwrap();
            if state.scheduled == Some(revision) {
                state.scheduled = None;
            }
            drop(state);
            log::error!("tray dispatch: {error}");
        }
    }
    fn connected(self: &Arc<Self>, visible: bool) {
        self.update(|state| {
            state.standalone = false;
            state.initial_pending = false;
            state.initial_preference = None;
            state.session += 1;
            Some(visible)
        });
    }
    fn disconnected(self: &Arc<Self>) {
        self.update(|state| {
            let visible = if state.initial_pending {
                state.initial_pending = false;
                state.initial_preference.take().unwrap_or(true)
            } else if state.standalone {
                state.desired.unwrap_or(true)
            } else {
                // A lost established session must actively restore access even when
                // the last completed UI operation already left the icon visible.
                state.applied = None;
                true
            };
            state.standalone = true;
            state.session += 1;
            Some(visible)
        });
    }
    fn request(self: &Arc<Self>, visible: bool) {
        self.update(|state| {
            if state.initial_pending {
                state.initial_preference = Some(visible);
                None
            } else {
                state.standalone.then_some(visible)
            }
        });
    }
    fn unified(self: &Arc<Self>, visible: bool) {
        self.update(|state| (!state.standalone).then_some(visible));
    }
    fn replace_menu(
        &self,
        items: Vec<MenuItem>,
        commands: &mpsc::UnboundedSender<Command>,
    ) -> Result<(), &'static str> {
        let mut state = self.state.lock().unwrap();
        if !state.active {
            return Err("client stopped");
        }
        commands
            .send(Command::Menu(items.clone()))
            .map_err(|_| "client stopped")?;
        state.menu = menu_ids(&items);
        Ok(())
    }
    fn dispatch_session(
        self: &Arc<Self>,
        name: &'static str,
        job: impl FnOnce(&Visual) + Send + 'static,
    ) {
        let session = {
            let state = self.state.lock().unwrap();
            if !state.active || state.standalone {
                return;
            }
            state.session
        };
        let visual = self.clone();
        if let Err(error) = self.host.dispatch(Box::new(move || {
            let valid = {
                let state = visual.state.lock().unwrap();
                state.active && !state.standalone && state.session == session
            };
            if valid {
                job(&visual);
            }
        })) {
            log::error!("{name} dispatch: {error}");
        }
    }
    fn action(self: &Arc<Self>, item_id: String) {
        if !self.state.lock().unwrap().menu.contains(&item_id) {
            return;
        }
        self.dispatch_session("action", move |visual| {
            let valid = visual.state.lock().unwrap().menu.contains(&item_id);
            if valid {
                visual.host.action(item_id);
            }
        });
    }
    fn show(self: &Arc<Self>) {
        self.dispatch_session("show", |visual| visual.host.show());
    }
    fn decide_quit(self: &Arc<Self>, reason: QuitReason, response: QuitResponder) {
        self.dispatch_session("quit", move |visual| {
            if response.live.load(Ordering::SeqCst) {
                visual.host.decide_quit(reason, response);
            }
        });
    }
    fn finalize_quit(self: &Arc<Self>) -> Result<(), Failure> {
        let stopped_session = {
            let mut state = self.state.lock().unwrap();
            if !state.active {
                return Err(Failure::Shutdown);
            }
            Self::fence(&mut state)
        };
        let visual = self.clone();
        if let Err(error) = self.host.dispatch(Box::new(move || {
            let valid = {
                let state = visual.state.lock().unwrap();
                !state.active && state.session == stopped_session
            };
            if valid {
                visual.host.finalize_quit();
            }
        })) {
            log::error!("quit finalization dispatch: {error}");
            let mut state = self.state.lock().unwrap();
            if !state.active && state.session == stopped_session {
                state.active = true;
                return Err(Failure::FinalizeDispatch);
            }
            return Err(Failure::Shutdown);
        }
        Ok(())
    }
    fn fence(state: &mut VisualState) -> u64 {
        state.active = false;
        state.revision += 1;
        state.session += 1;
        state.session
    }
    fn stop(&self) {
        Self::fence(&mut self.state.lock().unwrap());
    }
}

#[derive(Debug)]
enum Failure {
    Io(io::Error),
    Protocol(ProtocolError),
    Reject(RejectCode),
    Timeout,
    Shutdown,
    Finished,
    FinalizeDispatch,
}
impl From<io::Error> for Failure {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<ProtocolError> for Failure {
    fn from(value: ProtocolError) -> Self {
        Self::Protocol(value)
    }
}
impl Failure {
    fn permanent(&self) -> bool {
        matches!(
            self,
            Self::Reject(RejectCode::UnsupportedProtocol | RejectCode::DuplicateApp)
        )
    }
    fn report(&self) {
        match self {
            Self::Io(error) => log::debug!("hub connection: {error}"),
            Self::Protocol(error) => log::error!("hub protocol error: {error}"),
            Self::Reject(RejectCode::InvalidMessage) => {
                log::error!("hub rejected registration: invalid_message")
            }
            Self::Reject(RejectCode::UnsupportedProtocol) => {
                log::warn!("hub rejected connection: unsupported_protocol")
            }
            Self::Reject(RejectCode::DuplicateApp) => {
                log::warn!("hub rejected connection: duplicate_app")
            }
            Self::Reject(RejectCode::Other(_)) => {
                log::warn!("hub rejected connection with an unknown code")
            }
            Self::Timeout => log::debug!("hub connection timed out"),
            Self::Shutdown | Self::Finished | Self::FinalizeDispatch => {}
        }
    }
}

struct Frames {
    reader: FrameReader,
    writer: tokio::io::WriteHalf<Connection>,
}
struct FrameReader {
    stream: tokio::io::ReadHalf<Connection>,
    chunk: [u8; 4096],
    offset: usize,
    end: usize,
    line: Vec<u8>,
}
impl Frames {
    fn new(stream: Connection) -> Self {
        let (reader, writer) = tokio::io::split(stream);
        Self {
            reader: FrameReader {
                stream: reader,
                chunk: [0; 4096],
                offset: 0,
                end: 0,
                line: Vec::new(),
            },
            writer,
        }
    }
    async fn send(&mut self, message: &Message) -> Result<(), Failure> {
        let line = encode(PROTOCOL_VERSION, message)?;
        self.writer.write_all(line.as_bytes()).await?;
        Ok(())
    }
    async fn receive(&mut self) -> Result<Message, Failure> {
        self.reader.receive().await
    }
}
impl FrameReader {
    async fn receive(&mut self) -> Result<Message, Failure> {
        loop {
            if self.offset == self.end {
                self.end = self.stream.read(&mut self.chunk).await?;
                self.offset = 0;
                if self.end == 0 {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "pipe closed").into());
                }
            }
            let remaining = &self.chunk[self.offset..self.end];
            let newline = remaining.iter().position(|&b| b == b'\n');
            let length = newline.unwrap_or(remaining.len());
            let total = self.line.len() + length;
            let last = if length == 0 {
                self.line.last()
            } else {
                remaining.get(length - 1)
            };
            if total > MAX_LINE_BYTES + 1 || (total == MAX_LINE_BYTES + 1 && last != Some(&b'\r')) {
                return Err(ProtocolError::TooLarge { len: total }.into());
            }
            self.line.extend_from_slice(&remaining[..length]);
            self.offset += length + usize::from(newline.is_some());
            if newline.is_some() {
                let text = std::str::from_utf8(&self.line)
                    .map_err(|error| ProtocolError::Malformed(error.to_string()))?;
                let result = decode(text, PROTOCOL_VERSION).map_err(Failure::from);
                self.line.clear();
                return result;
            }
        }
    }
}

async fn handshake(
    connect: Connect,
    app: AppInfo,
    capabilities: Vec<Capability>,
) -> Result<(Frames, bool, Duration), Failure> {
    let mut frames = Frames::new(connect().await?);
    frames
        .send(&Message::Hello {
            protocol: VersionRange {
                min: PROTOCOL_VERSION,
                max: PROTOCOL_VERSION,
            },
        })
        .await?;
    let (unified, heartbeat) = match frames.receive().await? {
        Message::Welcome {
            protocol,
            unified,
            heartbeat_interval_ms,
        } => {
            if protocol != PROTOCOL_VERSION {
                return Err(ProtocolError::UnsupportedVersion {
                    found: protocol,
                    expected: PROTOCOL_VERSION,
                }
                .into());
            }
            (unified, heartbeat_interval_ms)
        }
        Message::Reject { code, .. } => return Err(Failure::Reject(code)),
        _ => return Err(ProtocolError::Malformed("expected welcome".into()).into()),
    };
    if !HEARTBEAT_INTERVAL_RANGE_MS.contains(&heartbeat) {
        return Err(ProtocolError::Malformed("invalid heartbeat interval".into()).into());
    }
    frames
        .send(&Message::Register { app, capabilities })
        .await?;
    match frames.receive().await? {
        Message::Registered => Ok((frames, unified, Duration::from_millis(heartbeat.into()))),
        Message::Reject { code, .. } => Err(Failure::Reject(code)),
        _ => Err(ProtocolError::Malformed("expected registered".into()).into()),
    }
}

#[derive(Clone, Copy)]
enum WriteTag {
    Ordinary,
    Refused(u64),
    Accepted(u64),
    Shutdown,
}
struct WriteRequest {
    messages: Vec<Message>,
    tag: WriteTag,
    deadline: tokio::time::Instant,
}
struct Writer {
    requests: mpsc::UnboundedSender<WriteRequest>,
    events: mpsc::UnboundedReceiver<(WriteTag, Result<(), Failure>)>,
    writing: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}
impl Writer {
    fn new(mut stream: tokio::io::WriteHalf<Connection>) -> Self {
        let (requests, mut incoming) = mpsc::unbounded_channel::<WriteRequest>();
        let (events, outgoing) = mpsc::unbounded_channel();
        let writing = Arc::new(AtomicBool::new(false));
        let writer_busy = writing.clone();
        let task = tokio::spawn(async move {
            while let Some(request) = incoming.recv().await {
                writer_busy.store(true, Ordering::SeqCst);
                let write = async {
                    for message in &request.messages {
                        let line = encode(PROTOCOL_VERSION, message)?;
                        stream.write_all(line.as_bytes()).await?;
                    }
                    Ok::<(), Failure>(())
                };
                // The deadline wins even if a writable pipe wakes at the same instant.
                let result = tokio::select! {
                    biased;
                    _ = tokio::time::sleep_until(request.deadline) => Err(Failure::Timeout),
                    result = write => result,
                };
                writer_busy.store(false, Ordering::SeqCst);
                let failed = result.is_err();
                if events.send((request.tag, result)).is_err() || failed {
                    break;
                }
            }
        });
        Self {
            requests,
            events: outgoing,
            writing,
            task,
        }
    }
    fn queue(
        &self,
        messages: Vec<Message>,
        tag: WriteTag,
        deadline: tokio::time::Instant,
    ) -> Result<(), Failure> {
        self.requests
            .send(WriteRequest {
                messages,
                tag,
                deadline,
            })
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "writer stopped").into())
    }
}
impl Drop for Writer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct PendingQuit {
    decision_by: tokio::time::Instant,
    reply_by: tokio::time::Instant,
    live: Arc<AtomicBool>,
}
impl Drop for PendingQuit {
    fn drop(&mut self) {
        self.live.store(false, Ordering::SeqCst);
    }
}

fn queue_quit_result(
    writer: &Writer,
    responding: &mut HashMap<u64, tokio::time::Instant>,
    id: u64,
    pending: PendingQuit,
    decision: QuitDecision,
) -> Result<bool, Failure> {
    if tokio::time::Instant::now() >= pending.reply_by {
        return Err(Failure::Timeout);
    }
    let (accepted, reason) = match decision {
        QuitDecision::Accept => (true, None),
        QuitDecision::Refuse(reason) => (false, Some(reason)),
    };
    let mut messages = vec![Message::QuitResult {
        id,
        accepted,
        reason,
    }];
    let tag = if accepted {
        messages.push(Message::Bye);
        WriteTag::Accepted(id)
    } else {
        WriteTag::Refused(id)
    };
    writer.queue(messages, tag, pending.reply_by)?;
    responding.insert(id, pending.reply_by);
    Ok(accepted)
}

fn apply_decision(
    writer: &Writer,
    pending: &mut HashMap<u64, PendingQuit>,
    responding: &mut HashMap<u64, tokio::time::Instant>,
    id: u64,
    decision: QuitDecision,
    decided_at: tokio::time::Instant,
) -> Result<bool, Failure> {
    let Some(quit) = pending.remove(&id) else {
        return Ok(false);
    };
    let decision = if decided_at >= quit.decision_by {
        QuitDecision::Refuse("Quit decision timed out".into())
    } else {
        decision
    };
    queue_quit_result(writer, responding, id, quit, decision)
}

fn handle_write_event(
    event: (WriteTag, Result<(), Failure>),
    responding: &mut HashMap<u64, tokio::time::Instant>,
    visual: &Arc<Visual>,
    stopping: bool,
) -> Option<Failure> {
    match event {
        (WriteTag::Ordinary, Ok(())) => None,
        (WriteTag::Refused(id), Ok(())) => {
            responding.remove(&id);
            None
        }
        (WriteTag::Accepted(id), Ok(())) => {
            responding.remove(&id);
            Some(match visual.finalize_quit() {
                Ok(()) => Failure::Finished,
                Err(failure) => failure,
            })
        }
        (WriteTag::Shutdown, Ok(())) => Some(Failure::Shutdown),
        (_, Err(error)) => Some(if stopping { Failure::Shutdown } else { error }),
    }
}

async fn session(
    frames: Frames,
    interval: Duration,
    menu: &mut Option<Vec<MenuItem>>,
    visual: &Arc<Visual>,
    commands: &mut mpsc::UnboundedReceiver<Command>,
    shutdown: &mut watch::Receiver<bool>,
) -> Failure {
    let Frames { reader, writer } = frames;
    let mut writer = Writer::new(writer);
    let result = run_session(
        reader,
        &mut writer,
        interval,
        menu,
        visual,
        commands,
        shutdown,
    )
    .await;
    writer.task.abort();
    let _ = (&mut writer.task).await;
    result
}

async fn run_session(
    mut reader: FrameReader,
    writer: &mut Writer,
    interval: Duration,
    menu: &mut Option<Vec<MenuItem>>,
    visual: &Arc<Visual>,
    commands: &mut mpsc::UnboundedReceiver<Command>,
    shutdown: &mut watch::Receiver<bool>,
) -> Failure {
    let write_limit = interval * MISSED_HEARTBEATS_LIMIT;
    if let Some(items) = menu.as_ref()
        && let Err(error) = writer.queue(
            vec![Message::Menu {
                items: items.clone(),
            }],
            WriteTag::Ordinary,
            tokio::time::Instant::now() + write_limit,
        )
    {
        return error;
    }
    let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let (decisions, mut decision_rx) = mpsc::unbounded_channel();
    let mut pending: HashMap<u64, PendingQuit> = HashMap::new();
    let mut responding: HashMap<u64, tokio::time::Instant> = HashMap::new();
    let mut seen = HashSet::new();
    let mut ending = false;
    let mut stopping = None;
    let mut seq = 0_u64;
    let mut acknowledged = 0_u64;
    let mut missed = 0_u32;
    loop {
        let next_deadline = pending
            .values()
            .map(|quit| quit.decision_by)
            .chain(responding.values().copied())
            .chain(stopping)
            .min();
        tokio::select! {
            _ = shutdown.changed(), if stopping.is_none() => {
                visual.stop();
                pending.clear();
                responding.clear();
                ending = true;
                if writer.writing.load(Ordering::SeqCst) { return Failure::Shutdown; }
                let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
                if writer.queue(vec![Message::Bye], WriteTag::Shutdown, deadline).is_err() { return Failure::Shutdown; }
                stopping = Some(deadline);
            }
            event = decision_rx.recv(), if !ending => {
                if let Some((id, decision, decided_at)) = event {
                    match apply_decision(writer, &mut pending, &mut responding, id, decision, decided_at) {
                        Ok(true) => { ending = true; pending.clear(); }
                        Ok(false) => {},
                        Err(error) => return error,
                    }
                }
            }
            _ = async {
                if let Some(deadline) = next_deadline { tokio::time::sleep_until(deadline).await; }
                else { std::future::pending::<()>().await; }
            } => {
                while !ending {
                    let Ok((id, decision, decided_at)) = decision_rx.try_recv() else { break; };
                    match apply_decision(writer, &mut pending, &mut responding, id, decision, decided_at) {
                        Ok(true) => { ending = true; pending.clear(); }
                        Ok(false) => {},
                        Err(error) => return error,
                    }
                }
                // A completed write can be waiting in the channel when its timer also wakes.
                while let Ok(event) = writer.events.try_recv() {
                    if let Some(result) = handle_write_event(event, &mut responding, visual, stopping.is_some()) { return result; }
                }
                let now = tokio::time::Instant::now();
                if stopping.is_some_and(|deadline| deadline <= now) { return Failure::Shutdown; }
                if responding.values().any(|deadline| *deadline <= now) { return Failure::Timeout; }
                if let Some(id) = pending.iter().filter(|(_, quit)| quit.decision_by <= now).min_by_key(|(_, quit)| quit.decision_by).map(|(&id, _)| id) {
                    let quit = pending.remove(&id).unwrap();
                    if let Err(error) = queue_quit_result(writer, &mut responding, id, quit, QuitDecision::Refuse("Quit decision timed out".into())) { return error; }
                }
            }
            event = writer.events.recv() => match event {
                Some(event) => {
                    if let Some(result) = handle_write_event(event, &mut responding, visual, stopping.is_some()) { return result; }
                },
                None => return if stopping.is_some() { Failure::Shutdown } else { Failure::Io(io::Error::new(io::ErrorKind::BrokenPipe, "writer stopped")) },
            },
            incoming = reader.receive(), if !ending => match incoming {
                Ok(Message::Unified { value }) => visual.unified(!value),
                Ok(Message::Action { item_id }) if menu.is_some() => visual.action(item_id),
                Ok(Message::Show) if visual.host.supports_show() => visual.show(),
                Ok(Message::Quit { id, reason }) if visual.host.supports_quit() => {
                    if seen.insert(id) {
                        let now = tokio::time::Instant::now();
                        let live = Arc::new(AtomicBool::new(true));
                        pending.insert(id, PendingQuit {
                            decision_by: now + Duration::from_millis(u64::from(QUIT_TIMEOUT_MS) - 1_000),
                            reply_by: now + Duration::from_millis(u64::from(QUIT_TIMEOUT_MS)),
                            live: live.clone(),
                        });
                        visual.decide_quit(reason, QuitResponder { id, sender: decisions.clone(), live });
                    }
                }
                Ok(Message::HeartbeatAck { seq: ack }) => {
                    if ack > acknowledged && ack <= seq { acknowledged = ack; missed = 0; }
                }
                Ok(_) => return Failure::Protocol(ProtocolError::Malformed("unexpected hub message".into())),
                Err(error) => return error,
            },
            command = commands.recv(), if !ending => match command {
                Some(Command::Menu(items)) if menu.is_some() => {
                    *menu = Some(items.clone());
                    if let Err(error) = writer.queue(vec![Message::Menu { items }], WriteTag::Ordinary, tokio::time::Instant::now() + write_limit) { return error; }
                }
                Some(Command::Menu(_)) => {},
                None => return Failure::Shutdown,
            },
            _ = timer.tick(), if !ending => {
                if seq > acknowledged { missed += 1; if missed >= MISSED_HEARTBEATS_LIMIT { return Failure::Timeout; } }
                seq = match seq.checked_add(1) { Some(next) => next, None => return Failure::Protocol(ProtocolError::Malformed("heartbeat sequence exhausted".into())) };
                if let Err(error) = writer.queue(vec![Message::Heartbeat { seq }], WriteTag::Ordinary, tokio::time::Instant::now() + write_limit) { return error; }
            }
        }
    }
}

async fn supervise(
    config: Config,
    visual: Arc<Visual>,
    connect: Connect,
    capabilities: Vec<Capability>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut menu = config.menu;
    let mut backoff = 1_u64;
    loop {
        let result = {
            let attempt = tokio::time::timeout(
                Duration::from_secs(1),
                handshake(connect.clone(), config.app.clone(), capabilities.clone()),
            );
            tokio::pin!(attempt);
            loop {
                tokio::select! {
                    _ = shutdown.changed() => { visual.stop(); return; }
                    command = commands.recv() => match command {
                        Some(Command::Menu(items)) if menu.is_some() => { menu = Some(items); }
                        Some(_) => {},
                        None => { visual.stop(); return; }
                    },
                    result = &mut attempt => break result.map_err(|_| Failure::Timeout).and_then(|value| value),
                }
            }
        };
        let failure = match result {
            Ok((frames, unified, interval)) => {
                backoff = 1;
                visual.connected(!unified);
                session(
                    frames,
                    interval,
                    &mut menu,
                    &visual,
                    &mut commands,
                    &mut shutdown,
                )
                .await
            }
            Err(error) => error,
        };
        if matches!(failure, Failure::Finished) {
            return;
        }
        if matches!(failure, Failure::Shutdown) {
            visual.stop();
            return;
        }
        failure.report();
        let permanent = failure.permanent();
        visual.disconnected();
        let delay = tokio::time::sleep(Duration::from_secs(backoff));
        tokio::pin!(delay);
        loop {
            tokio::select! {
                _ = shutdown.changed() => { visual.stop(); return; }
                command = commands.recv() => match command {
                    Some(Command::Menu(items)) if menu.is_some() => { menu = Some(items); }
                    Some(_) => {},
                    None => { visual.stop(); return; }
                },
                _ = &mut delay, if !permanent => break,
            }
        }
        backoff = (backoff * 2).min(10);
    }
}

pub(crate) fn start(config: Config, host: Arc<dyn Host>, connect: Connect) -> Handle {
    let (commands, receiver) = mpsc::unbounded_channel();
    let (shutdown, shutdown_receiver) = watch::channel(false);
    let (done_sender, done) = watch::channel(false);
    let has_menu = config.menu.is_some();
    let mut capabilities = Vec::new();
    if host.supports_show() {
        capabilities.push(Capability::Show);
    }
    if host.supports_quit() {
        capabilities.push(Capability::Quit);
    }
    if has_menu {
        capabilities.push(Capability::Menu);
    }
    let initial_menu = menu_ids(config.menu.as_deref().unwrap_or_default());
    let visual = Arc::new(Visual {
        host,
        state: Mutex::new(VisualState {
            revision: 0,
            session: 0,
            active: true,
            standalone: false,
            initial_pending: true,
            initial_preference: None,
            busy: false,
            desired: None,
            applied: None,
            scheduled: None,
            menu: initial_menu,
            icon: None,
        }),
    });
    let supervisor_visual = visual.clone();
    let task = async move {
        supervise(
            config,
            supervisor_visual,
            connect,
            capabilities,
            receiver,
            shutdown_receiver,
        )
        .await;
        done_sender.send_replace(true);
    };
    #[cfg(test)]
    tokio::spawn(task);
    #[cfg(not(test))]
    tauri::async_runtime::spawn(task);
    Handle {
        commands,
        shutdown,
        done,
        visual,
        has_menu,
    }
}
