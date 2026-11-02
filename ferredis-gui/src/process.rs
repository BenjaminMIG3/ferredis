//! Cycle de vie du binaire `ferredis-server` lancé depuis le GUI.
//!
//! Responsabilité unique : résoudre le binaire, démarrer / arrêter le process.
//! Le redémarrage (changement de port inclus) réutilise strictement stop + start.
//!
//! Sous Windows, le process est rattaché à un Job Object `KILL_ON_JOB_CLOSE` :
//! même un Ctrl+C / kill du GUI coupe le serveur (les `Drop` Rust ne tournent pas).

use crate::net::Conn;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Paramètres d'écoute du serveur géré (value object immuable).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerSpec {
    pub bind: String,
    pub port: u16,
}

/// Processus `ferredis-server` dont le GUI possède le cycle de vie.
pub struct ManagedServer {
    child: Option<Child>,
    spec: Option<ServerSpec>,
    /// Garde le job vivant tant que le GUI vit (Windows).
    #[cfg(windows)]
    job: Option<kill_on_close::Job>,
}

impl Default for ManagedServer {
    fn default() -> Self {
        Self {
            child: None,
            spec: None,
            #[cfg(windows)]
            job: kill_on_close::Job::new(),
        }
    }
}

impl ManagedServer {
    pub fn is_running(&mut self) -> bool {
        self.reap_exited();
        self.child.is_some()
    }

    pub fn running_spec(&self) -> Option<&ServerSpec> {
        self.spec.as_ref()
    }

    pub fn start(&mut self, spec: ServerSpec) -> Result<(), String> {
        self.reap_exited();
        if self.child.is_some() {
            return Err("un serveur local est déjà lancé".into());
        }

        let bin = resolve_server_binary()?;
        let data_dir = persistence_dir(&bin);
        let mut cmd = Command::new(&bin);
        cmd.arg("--bind")
            .arg(&spec.bind)
            .arg("--port")
            .arg(spec.port.to_string())
            // Répertoire de données stable (indépendant du CWD du GUI).
            .arg("--dir")
            .arg(&data_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        hide_console(&mut cmd);
        #[cfg(unix)]
        install_parent_death_signal(&mut cmd);

        let child = cmd
            .spawn()
            .map_err(|e| format!("impossible de démarrer {}: {e}", bin.display()))?;

        #[cfg(windows)]
        if let Some(job) = &self.job {
            let _ = job.assign(&child);
        }

        self.child = Some(child);
        self.spec = Some(spec);
        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), String> {
        self.reap_exited();
        let Some(mut child) = self.child.take() else {
            self.spec = None;
            return Ok(());
        };
        let spec = self.spec.take();

        if let Some(s) = &spec {
            try_graceful_shutdown(&s.bind, s.port);
        }

        wait_or_kill(&mut child, Duration::from_secs(3))?;
        Ok(())
    }

    /// Redémarrage simple : arrêt puis relance (changement de port inclus).
    pub fn restart(&mut self, spec: ServerSpec) -> Result<(), String> {
        self.stop()?;
        // Laisse le port se libérer avant le rebind.
        std::thread::sleep(Duration::from_millis(150));
        self.start(spec)
    }

    fn reap_exited(&mut self) {
        let exited = match self.child.as_mut() {
            Some(c) => matches!(c.try_wait(), Ok(Some(_)) | Err(_)),
            None => false,
        };
        if exited {
            self.child = None;
            self.spec = None;
        }
    }
}

impl Drop for ManagedServer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn try_graceful_shutdown(host: &str, port: u16) {
    let Ok(mut conn) = Conn::open(
        host,
        port,
        "ferredis-gui-shutdown",
        // Laisser le temps à SAVE RDB synchrone (fichiers volumineux).
        Duration::from_secs(5),
    ) else {
        return;
    };
    // Parité Redis / Ferredis : SHUTDOWN sans NOSAVE → flush RDB avant exit.
    // (L'ancien `SHUTDOWN NOSAVE` expliquait la perte de clés au redémarrage GUI.)
    let _ = conn.call(&["SHUTDOWN", "SAVE"]);
}

/// Dossier de persistance : à côté du binaire serveur (stable entre redémarrages).
fn persistence_dir(server_bin: &Path) -> PathBuf {
    server_bin
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."))
}

fn wait_or_kill(child: &mut Child, budget: Duration) -> Result<(), String> {
    let deadline = Instant::now() + budget;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return Ok(()),
            Ok(None) if Instant::now() >= deadline => break,
            Ok(None) => std::thread::sleep(Duration::from_millis(40)),
            Err(e) => return Err(e.to_string()),
        }
    }
    child.kill().map_err(|e| e.to_string())?;
    let _ = child.wait();
    Ok(())
}

/// Résout le binaire à côté du GUI, puis dans `target/{debug,release}`, puis le PATH.
fn resolve_server_binary() -> Result<PathBuf, String> {
    let name = server_binary_name();
    let mut candidates = Vec::with_capacity(6);

    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            candidates.push(dir.join(name));
        }
    }

    // Développement : `cargo run -p ferredis-gui` depuis la racine du workspace.
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    if let Some(workspace) = manifest_dir.parent() {
        candidates.push(workspace.join("target/debug").join(name));
        candidates.push(workspace.join("target/release").join(name));
    }

    for path in &candidates {
        if path.is_file() {
            return Ok(path.clone());
        }
    }

    // Dernier recours : laisser l'OS chercher dans le PATH.
    Ok(PathBuf::from(name))
}

fn server_binary_name() -> &'static str {
    if cfg!(windows) {
        "ferredis-server.exe"
    } else {
        "ferredis-server"
    }
}

fn hide_console(cmd: &mut Command) {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }
    let _ = cmd;
}

/// Linux : le fils reçoit SIGTERM si le GUI meurt (Ctrl+C inclus).
#[cfg(unix)]
fn install_parent_death_signal(cmd: &mut Command) {
    use std::os::unix::process::CommandExt;
    unsafe {
        cmd.pre_exec(|| {
            #[cfg(target_os = "linux")]
            {
                const PR_SET_PDEATHSIG: i32 = 1;
                const SIGTERM: i32 = 15;
                libc_prctl(PR_SET_PDEATHSIG, SIGTERM, 0, 0, 0);
            }
            Ok(())
        });
    }
}

#[cfg(all(unix, target_os = "linux"))]
unsafe fn libc_prctl(option: i32, arg2: i32, arg3: i32, arg4: i32, arg5: i32) -> i32 {
    extern "C" {
        fn prctl(option: i32, ...) -> i32;
    }
    prctl(option, arg2, arg3, arg4, arg5)
}

/// Job Windows : fermeture du handle (sortie du GUI) → kill des process enfants.
#[cfg(windows)]
mod kill_on_close {
    use std::os::windows::io::AsRawHandle;
    use std::process::Child;
    use std::ptr;

    type Handle = *mut core::ffi::c_void;
    type Bool = i32;
    type Dword = u32;

    const JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE: Dword = 0x2000;
    const JOB_OBJECT_EXTENDED_LIMIT_INFORMATION: i32 = 9;

    #[repr(C)]
    struct BasicLimitInformation {
        per_process_user_time_limit: i64,
        per_job_user_time_limit: i64,
        limit_flags: Dword,
        minimum_working_set_size: usize,
        maximum_working_set_size: usize,
        active_process_limit: Dword,
        affinity: usize,
        priority_class: Dword,
        scheduling_class: Dword,
    }

    #[repr(C)]
    struct IoCounters {
        read_operation_count: u64,
        write_operation_count: u64,
        other_operation_count: u64,
        read_transfer_count: u64,
        write_transfer_count: u64,
        other_transfer_count: u64,
    }

    #[repr(C)]
    struct ExtendedLimitInformation {
        basic_limit_information: BasicLimitInformation,
        io_info: IoCounters,
        process_memory_limit: usize,
        job_memory_limit: usize,
        peak_process_memory_used: usize,
        peak_job_memory_used: usize,
    }

    #[link(name = "kernel32")]
    extern "system" {
        fn CreateJobObjectW(attrs: *mut core::ffi::c_void, name: *const u16) -> Handle;
        fn SetInformationJobObject(
            job: Handle,
            class: i32,
            info: *mut core::ffi::c_void,
            len: Dword,
        ) -> Bool;
        fn AssignProcessToJobObject(job: Handle, process: Handle) -> Bool;
        fn CloseHandle(handle: Handle) -> Bool;
    }

    pub struct Job(Handle);

    impl Job {
        pub fn new() -> Option<Self> {
            unsafe {
                let handle = CreateJobObjectW(ptr::null_mut(), ptr::null());
                if handle.is_null() {
                    return None;
                }
                let mut info: ExtendedLimitInformation = std::mem::zeroed();
                info.basic_limit_information.limit_flags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
                let ok = SetInformationJobObject(
                    handle,
                    JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
                    (&mut info as *mut ExtendedLimitInformation).cast(),
                    std::mem::size_of::<ExtendedLimitInformation>() as Dword,
                );
                if ok == 0 {
                    CloseHandle(handle);
                    return None;
                }
                Some(Self(handle))
            }
        }

        pub fn assign(&self, child: &Child) -> bool {
            unsafe { AssignProcessToJobObject(self.0, child.as_raw_handle() as Handle) != 0 }
        }
    }

    impl Drop for Job {
        fn drop(&mut self) {
            unsafe {
                CloseHandle(self.0);
            }
        }
    }
}
