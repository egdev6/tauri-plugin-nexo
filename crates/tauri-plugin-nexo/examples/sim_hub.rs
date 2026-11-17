//! Interactive manual-test hub. It deliberately uses the production pipe, not a test endpoint.

#[cfg(not(windows))]
fn main() {
    println!("sim_hub is Windows-only");
}

#[cfg(windows)]
fn main() -> std::io::Result<()> {
    sim::run()
}

#[cfg(windows)]
mod sim {
    use std::{
        ffi::c_void,
        io::{self, BufRead},
        ptr,
    };

    use nexo_protocol::{
        Capability, DEFAULT_HEARTBEAT_INTERVAL_MS, MAX_LINE_BYTES, Message, PROTOCOL_VERSION,
        QuitReason, RejectCode, VersionRange, decode, encode, pipe_name,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
        net::windows::named_pipe::{NamedPipeServer, ServerOptions},
        sync::mpsc,
    };
    use windows_sys::Win32::{
        Foundation::LocalFree,
        Security::{
            Authorization::{
                ConvertStringSecurityDescriptorToSecurityDescriptorW, SDDL_REVISION_1,
            },
            SECURITY_ATTRIBUTES,
        },
    };

    const HELP: &str = "Commands: u = toggle unified, s = show, q = quit, a <id> = menu action, d = drop connection, x = exit";

    // The converted security descriptor is allocated by Windows and must outlive CreateNamedPipe.
    struct Descriptor(*mut c_void);
    impl Drop for Descriptor {
        fn drop(&mut self) {
            unsafe { LocalFree(self.0) };
        }
    }

    fn listen() -> io::Result<(NamedPipeServer, String)> {
        let sid = tauri_plugin_nexo::current_user_sid()?;
        let name = pipe_name(&sid);
        // Deny inherited ACEs; grant the current user full access, and nobody else.
        let sddl: Vec<u16> = format!("D:P(A;;GA;;;{sid})\0").encode_utf16().collect();
        let mut raw = ptr::null_mut();
        // SAFETY: sddl is NUL-terminated and raw is a valid output pointer.
        if unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl.as_ptr(),
                SDDL_REVISION_1,
                &mut raw,
                ptr::null_mut(),
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let descriptor = Descriptor(raw);
        let mut attrs = SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: descriptor.0,
            bInheritHandle: 0,
        };
        // SAFETY: attrs and its descriptor remain valid until pipe creation returns.
        let server = unsafe {
            ServerOptions::new()
                .first_pipe_instance(true)
                .max_instances(2)
                .create_with_security_attributes_raw(
                    &name,
                    (&mut attrs as *mut SECURITY_ATTRIBUTES).cast(),
                )
        }
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("cannot bind {name}; another hub may already own the pipe: {error}"),
            )
        })?;
        println!("Listening on {name} (current user only)");
        Ok((server, name))
    }

    async fn send(writer: &mut (impl AsyncWrite + Unpin), message: Message) -> io::Result<()> {
        let line = encode(PROTOCOL_VERSION, &message).map_err(io::Error::other)?;
        writer.write_all(line.as_bytes()).await?;
        println!("-> {}", line.trim_end_matches('\n'));
        Ok(())
    }

    enum Phase {
        Hello,
        Register,
        Ready(Vec<Capability>),
    }

    enum End {
        Disconnect,
        Exit,
    }

    async fn session(
        server: &mut NamedPipeServer,
        commands: &mut mpsc::UnboundedReceiver<String>,
        unified: &mut bool,
        quit_id: &mut u64,
    ) -> io::Result<End> {
        let (mut reader, mut writer) = tokio::io::split(server);
        let mut phase = Phase::Hello;
        let mut pending = Vec::new();
        let mut buffer = [0_u8; 4096];
        loop {
            tokio::select! {
                input = commands.recv() => {
                    let Some(input) = input else { return Ok(End::Exit) };
                    let input = input.trim();
                    if input == "x" { return Ok(End::Exit); }
                    if input == "d" {
                        println!("Dropping connection; waiting for reconnect");
                        return Ok(End::Disconnect);
                    }
                    let Phase::Ready(capabilities) = &phase else {
                        println!("App is not registered yet");
                        continue;
                    };
                    match input {
                        "u" => {
                            let value = !*unified;
                            send(&mut writer, Message::Unified { value }).await?;
                            *unified = value;
                            println!("Unified: {value}");
                        }
                        "s" if capabilities.contains(&Capability::Show) => {
                            send(&mut writer, Message::Show).await?;
                        }
                        "q" if capabilities.contains(&Capability::Quit) => {
                            *quit_id = quit_id.checked_add(1).ok_or_else(|| io::Error::other("quit id exhausted"))?;
                            send(&mut writer, Message::Quit { id: *quit_id, reason: QuitReason::User }).await?;
                        }
                        _ if input.starts_with("a ") && capabilities.contains(&Capability::Menu) => {
                            let id = input[2..].trim();
                            if id.is_empty() { println!("Usage: a <id>"); }
                            else { send(&mut writer, Message::Action { item_id: id.into() }).await?; }
                        }
                        _ => println!("Unknown command or unsupported capability. {HELP}"),
                    }
                }
                read = reader.read(&mut buffer) => {
                    let count = read?;
                    if count == 0 {
                        println!("App disconnected");
                        return Ok(End::Disconnect);
                    }
                    for &byte in &buffer[..count] {
                        if byte != b'\n' {
                            pending.push(byte);
                            if pending.len() > MAX_LINE_BYTES + 1
                                || (pending.len() == MAX_LINE_BYTES + 1 && byte != b'\r')
                            {
                                return Err(io::Error::new(io::ErrorKind::InvalidData, "frame exceeds protocol limit"));
                            }
                            continue;
                        }
                        println!("<- {}", String::from_utf8_lossy(&pending));
                        let line = std::str::from_utf8(&pending).map_err(io::Error::other)?;
                        let message = decode(line, PROTOCOL_VERSION).map_err(io::Error::other)?;
                        pending.clear();
                        match message {
                            Message::Hello { protocol } if matches!(phase, Phase::Hello) => {
                                if !(protocol.min..=protocol.max).contains(&PROTOCOL_VERSION) {
                                    send(&mut writer, Message::Reject {
                                        code: RejectCode::UnsupportedProtocol,
                                        hub_protocol: Some(VersionRange { min: PROTOCOL_VERSION, max: PROTOCOL_VERSION }),
                                    }).await?;
                                    return Ok(End::Disconnect);
                                }
                                send(&mut writer, Message::Welcome {
                                    protocol: PROTOCOL_VERSION,
                                    unified: *unified,
                                    heartbeat_interval_ms: DEFAULT_HEARTBEAT_INTERVAL_MS,
                                }).await?;
                                phase = Phase::Register;
                            }
                            Message::Register { app, capabilities } if matches!(phase, Phase::Register) => {
                                println!("Registered app: {} / {} / {}", app.id, app.name, app.version);
                                send(&mut writer, Message::Registered).await?;
                                phase = Phase::Ready(capabilities);
                            }
                            Message::Menu { items } if matches!(phase, Phase::Ready(_)) => {
                                println!("Menu replacement: {items:?}");
                            }
                            Message::Heartbeat { seq } if matches!(phase, Phase::Ready(_)) => {
                                send(&mut writer, Message::HeartbeatAck { seq }).await?;
                            }
                            Message::QuitResult { .. } if matches!(phase, Phase::Ready(_)) => {}
                            Message::Bye if matches!(phase, Phase::Ready(_)) => {
                                println!("App sent bye");
                                return Ok(End::Disconnect);
                            }
                            _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected protocol message")),
                        }
                    }
                }
            }
        }
    }

    #[tokio::main(flavor = "current_thread")]
    pub async fn run() -> io::Result<()> {
        let mut unified = true;
        for arg in std::env::args().skip(1) {
            match arg.as_str() {
                "--standalone" => unified = false,
                "--help" | "-h" => {
                    println!("sim_hub [--standalone]\n{HELP}");
                    return Ok(());
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("unknown argument: {arg}"),
                    ));
                }
            }
        }
        let (mut server, name) = listen()?;
        println!("Unified at welcome: {unified}\n{HELP}");
        let (sender, mut commands) = mpsc::unbounded_channel();
        std::thread::spawn(move || {
            for line in io::stdin().lock().lines() {
                let Ok(line) = line else { break };
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        let mut quit_id = 0;
        loop {
            tokio::select! {
                input = commands.recv() => match input.as_deref().map(str::trim) {
                    None | Some("x") => break,
                    Some(_) => println!("No app connected. {HELP}"),
                },
                connected = server.connect() => {
                    connected?;
                    // Reserve the pipe with a fresh listener before dropping this session.
                    let next = ServerOptions::new().max_instances(2).create(&name)?;
                    let mut client = std::mem::replace(&mut server, next);
                    println!("App connected");
                    let result = session(&mut client, &mut commands, &mut unified, &mut quit_id).await;
                    drop(client);
                    match result {
                        Ok(End::Exit) => break,
                        Ok(End::Disconnect) => {}
                        Err(error) => eprintln!("Session ended: {error}"),
                    }
                    println!("Waiting for an app");
                }
            }
        }
        println!("sim_hub stopped");
        Ok(())
    }
}
