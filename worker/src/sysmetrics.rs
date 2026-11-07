//! CPU load, memory use and the host name, read straight from the operating system (no dependency needed).

/// The name of this computer.
pub fn host_name() -> Option<String> {
    #[cfg(windows)]
    {
        std::env::var("COMPUTERNAME").ok().filter(|n| !n.is_empty())
    }
    #[cfg(not(windows))]
    {
        std::fs::read_to_string("/proc/sys/kernel/hostname")
            .ok()
            .map(|n| n.trim().to_string())
            .filter(|n| !n.is_empty())
            .or_else(|| std::env::var("HOSTNAME").ok().filter(|n| !n.is_empty()))
    }
}

/// CPU and RAM of the whole machine, as percentages.
pub struct Sampler {
    last_cpu: Option<(u64, u64)>, // (busy, total) ticks at the previous sample
}

impl Sampler {
    pub fn new() -> Sampler {
        Sampler {
            last_cpu: cpu_ticks(),
        }
    }

    /// (cpu percent, ram percent); None when the system cannot say. The CPU figure covers the time since the previous call.
    pub fn sample(&mut self) -> Option<(f64, f64)> {
        let now = cpu_ticks()?;
        let ram = ram_percent()?;
        let cpu = match self.last_cpu {
            Some((busy, total)) if now.1 > total => {
                100.0 * (now.0.saturating_sub(busy)) as f64 / (now.1 - total) as f64
            }
            _ => 0.0,
        };
        self.last_cpu = Some(now);
        Some((cpu.clamp(0.0, 100.0), ram.clamp(0.0, 100.0)))
    }
}

#[cfg(target_os = "linux")]
fn cpu_ticks() -> Option<(u64, u64)> {
    let stat = std::fs::read_to_string("/proc/stat").ok()?;
    let line = stat.lines().next()?;
    let values: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .filter_map(|v| v.parse().ok())
        .collect();
    if values.len() < 4 {
        return None;
    }
    let total: u64 = values.iter().take(8).sum(); // user nice system idle iowait irq softirq steal
    let idle = values[3] + values.get(4).copied().unwrap_or(0);
    Some((total - idle, total))
}

#[cfg(target_os = "linux")]
fn ram_percent() -> Option<f64> {
    let info = std::fs::read_to_string("/proc/meminfo").ok()?;
    let field = |name: &str| {
        info.lines()
            .find(|l| l.starts_with(name))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse::<f64>().ok())
    };
    let (total, available) = (field("MemTotal:")?, field("MemAvailable:")?);
    if total <= 0.0 {
        return None;
    }
    Some(100.0 * (1.0 - available / total))
}

#[cfg(windows)]
fn cpu_ticks() -> Option<(u64, u64)> {
    use windows_sys::Win32::Foundation::FILETIME;
    use windows_sys::Win32::System::Threading::GetSystemTimes;
    let zero = FILETIME {
        dwLowDateTime: 0,
        dwHighDateTime: 0,
    };
    let (mut idle, mut kernel, mut user) = (zero, zero, zero);
    // SAFETY: three valid FILETIME out-pointers
    if unsafe { GetSystemTimes(&mut idle, &mut kernel, &mut user) } == 0 {
        return None;
    }
    let value = |t: FILETIME| ((t.dwHighDateTime as u64) << 32) | t.dwLowDateTime as u64;
    let (idle, total) = (value(idle), value(kernel) + value(user)); // kernel time includes idle time
    Some((total - idle, total))
}

#[cfg(windows)]
fn ram_percent() -> Option<f64> {
    use windows_sys::Win32::System::SystemInformation::{GlobalMemoryStatusEx, MEMORYSTATUSEX};
    // SAFETY: a zeroed MEMORYSTATUSEX with its length set, as the API requires
    let mut status: MEMORYSTATUSEX = unsafe { std::mem::zeroed() };
    status.dwLength = std::mem::size_of::<MEMORYSTATUSEX>() as u32;
    if unsafe { GlobalMemoryStatusEx(&mut status) } == 0 || status.ullTotalPhys == 0 {
        return None;
    }
    Some(100.0 * (1.0 - status.ullAvailPhys as f64 / status.ullTotalPhys as f64))
}

#[cfg(not(any(target_os = "linux", windows)))]
fn cpu_ticks() -> Option<(u64, u64)> {
    None
}

#[cfg(not(any(target_os = "linux", windows)))]
fn ram_percent() -> Option<f64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_machine_reports_sane_numbers() {
        let mut sampler = Sampler::new();
        std::thread::sleep(std::time::Duration::from_millis(300));
        let (cpu, ram) = sampler.sample().expect("this OS reports CPU and RAM");
        assert!(
            (0.0..=100.0).contains(&cpu) && ram > 0.0 && ram <= 100.0,
            "cpu {cpu} ram {ram}"
        );
        assert!(host_name().is_some());
    }
}
