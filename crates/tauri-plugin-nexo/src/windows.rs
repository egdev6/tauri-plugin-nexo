use std::{io, ptr, sync::Arc};

use nexo_protocol::{AppInfo, MenuItem, QuitReason, pipe_name};
use tauri::{
    AppHandle, Manager, RunEvent, Runtime,
    plugin::{Builder, TauriPlugin},
    tray::TrayIcon,
};
use tokio::net::windows::named_pipe::ClientOptions;
use windows_sys::Win32::{
    Foundation::{CloseHandle, HANDLE, LocalFree},
    Security::{
        Authorization::ConvertSidToStringSidW, GetTokenInformation, TOKEN_QUERY, TOKEN_USER,
        TokenUser,
    },
    System::Threading::{GetCurrentProcess, OpenProcessToken},
};

use crate::client::{self, Config, Connect, Connection, Handle, Host, Icon, Job, QuitResponder};

type Factory<R> = Arc<dyn Fn(&AppHandle<R>) -> tauri::Result<TrayIcon<R>> + Send + Sync>;
type Action<R> = Arc<dyn Fn(&AppHandle<R>, &str) + Send + Sync>;
type Show<R> = Arc<dyn Fn(&AppHandle<R>) + Send + Sync>;
type QuitDecisionCallback<R> = Arc<dyn Fn(&AppHandle<R>, QuitReason, QuitResponder) + Send + Sync>;
type QuitFinalizeCallback<R> = Arc<dyn Fn(&AppHandle<R>) + Send + Sync>;

/// Host-owned tray and optional menu integration for Windows.
pub struct Options<R: Runtime> {
    app: AppInfo,
    factory: Factory<R>,
    menu: Option<Vec<MenuItem>>,
    action: Option<Action<R>>,
    show: Option<Show<R>>,
    quit: Option<(QuitDecisionCallback<R>, QuitFinalizeCallback<R>)>,
}

impl<R: Runtime> Options<R> {
    /// Supply the app identity and a factory that creates its normal, visible tray icon.
    /// The factory runs lazily on Tauri's main thread and must create at most one icon per call.
    pub fn new(
        app: AppInfo,
        factory: impl Fn(&AppHandle<R>) -> tauri::Result<TrayIcon<R>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            app,
            factory: Arc::new(factory),
            menu: None,
            action: None,
            show: None,
            quit: None,
        }
    }

    /// Enable `show`; the callback must show, unminimize and focus the host's main window.
    pub fn show(mut self, callback: impl Fn(&AppHandle<R>) + Send + Sync + 'static) -> Self {
        self.show = Some(Arc::new(callback));
        self
    }

    /// Enable `quit` using a nonblocking decision callback and a separate final-exit callback.
    pub fn quit(
        mut self,
        decide: impl Fn(&AppHandle<R>, QuitReason, QuitResponder) + Send + Sync + 'static,
        finalize: impl Fn(&AppHandle<R>) + Send + Sync + 'static,
    ) -> Self {
        self.quit = Some((Arc::new(decide), Arc::new(finalize)));
        self
    }

    /// Enable the menu capability with a replacement list and an action callback.
    pub fn menu(
        mut self,
        items: Vec<MenuItem>,
        action: impl Fn(&AppHandle<R>, &str) + Send + Sync + 'static,
    ) -> Result<Self, &'static str> {
        client::validate_menu(&items)?;
        self.menu = Some(items);
        self.action = Some(Arc::new(action));
        Ok(self)
    }
}

struct Managed(Handle);

/// Return the configured plugin's handle after Tauri setup, if installed.
pub fn handle<R: Runtime>(app: &AppHandle<R>) -> Option<Handle> {
    app.try_state::<Managed>().map(|state| state.0.clone())
}

/// Install the configured Windows client without blocking application startup.
pub fn configured<R: Runtime>(options: Options<R>) -> TauriPlugin<R> {
    Builder::new("nexo")
        .setup(move |app, _api| {
            let host = Arc::new(TauriHost {
                app: app.clone(),
                factory: options.factory,
                action: options.action,
                show: options.show,
                quit: options.quit,
            });
            let client = client::start(
                Config {
                    app: options.app,
                    menu: options.menu,
                },
                host,
                production_connector(),
            );
            if !app.manage(Managed(client.clone())) {
                client.shutdown();
                return Err("Nexo client state already exists".into());
            }
            Ok(())
        })
        .on_event(|app, event| {
            if matches!(event, RunEvent::Exit)
                && let Some(handle) = handle(app)
            {
                handle.shutdown();
            }
        })
        .on_drop(|app| {
            if let Some(handle) = handle(&app) {
                handle.shutdown();
            }
        })
        .build()
}

struct TauriHost<R: Runtime> {
    app: AppHandle<R>,
    factory: Factory<R>,
    action: Option<Action<R>>,
    show: Option<Show<R>>,
    quit: Option<(QuitDecisionCallback<R>, QuitFinalizeCallback<R>)>,
}
struct TauriIcon<R: Runtime>(TrayIcon<R>);
impl<R: Runtime> Icon for TauriIcon<R> {
    fn set_visible(&self, visible: bool) -> Result<(), String> {
        self.0
            .set_visible(visible)
            .map_err(|error| error.to_string())
    }
}
impl<R: Runtime> Host for TauriHost<R> {
    fn dispatch(&self, job: Job) -> Result<(), String> {
        self.app
            .run_on_main_thread(job)
            .map_err(|error| error.to_string())
    }
    fn create_icon(&self) -> Result<Box<dyn Icon>, String> {
        (self.factory)(&self.app)
            .map(|icon| Box::new(TauriIcon(icon)) as Box<dyn Icon>)
            .map_err(|error| error.to_string())
    }
    fn action(&self, item_id: String) {
        if let Some(action) = &self.action {
            action(&self.app, &item_id);
        }
    }
    fn supports_show(&self) -> bool {
        self.show.is_some()
    }
    fn supports_quit(&self) -> bool {
        self.quit.is_some()
    }
    fn show(&self) {
        if let Some(show) = &self.show {
            show(&self.app);
        }
    }
    fn decide_quit(&self, reason: QuitReason, response: QuitResponder) {
        if let Some((decide, _)) = &self.quit {
            decide(&self.app, reason, response);
        }
    }
    fn finalize_quit(&self) {
        if let Some((_, finalize)) = &self.quit {
            finalize(&self.app);
        }
    }
}

fn open(endpoint: &str) -> io::Result<Connection> {
    ClientOptions::new()
        .open(endpoint)
        .map(|pipe| Box::new(pipe) as Connection)
}

fn production_connector() -> Connect {
    Arc::new(|| {
        Box::pin(async {
            let sid = current_user_sid()?;
            open(&pipe_name(&sid))
        })
    })
}

// This injection is compiled only into this crate's tests; it cannot name a real hub pipe.
#[cfg(test)]
pub(crate) fn test_connector(endpoint: String) -> Connect {
    assert!(endpoint.starts_with(r"\\.\pipe\nexo-test-"));
    Arc::new(move || {
        let endpoint = endpoint.clone();
        Box::pin(async move { open(&endpoint) })
    })
}

struct Token(HANDLE);
impl Drop for Token {
    fn drop(&mut self) {
        unsafe {
            CloseHandle(self.0);
        }
    }
}
struct LocalString(*mut u16);
impl Drop for LocalString {
    fn drop(&mut self) {
        unsafe {
            LocalFree(self.0.cast());
        }
    }
}

/// Return the Windows user SID used for production hub discovery.
#[doc(hidden)]
pub fn current_user_sid() -> io::Result<String> {
    unsafe {
        let mut raw_token: HANDLE = ptr::null_mut();
        if OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut raw_token) == 0 {
            return Err(io::Error::last_os_error());
        }
        let token = Token(raw_token);
        let mut bytes = 0_u32;
        GetTokenInformation(token.0, TokenUser, ptr::null_mut(), 0, &mut bytes);
        if bytes == 0 {
            return Err(io::Error::last_os_error());
        }
        // TOKEN_USER contains pointers and therefore requires pointer-aligned storage.
        let words = (bytes as usize).div_ceil(std::mem::size_of::<usize>());
        let mut buffer = vec![0_usize; words];
        if GetTokenInformation(
            token.0,
            TokenUser,
            buffer.as_mut_ptr().cast(),
            bytes,
            &mut bytes,
        ) == 0
        {
            return Err(io::Error::last_os_error());
        }
        let user = &*buffer.as_ptr().cast::<TOKEN_USER>();
        let mut raw_string = ptr::null_mut();
        if ConvertSidToStringSidW(user.User.Sid, &mut raw_string) == 0 {
            return Err(io::Error::last_os_error());
        }
        let string = LocalString(raw_string);
        let mut length = 0;
        while *string.0.add(length) != 0 {
            length += 1;
        }
        String::from_utf16(std::slice::from_raw_parts(string.0, length))
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid SID encoding"))
    }
}
