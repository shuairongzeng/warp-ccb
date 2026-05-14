use std::io::Write;

fn main() {
    let mut channel = "local";
    let mut target_os = "windows";
    let mut args = std::env::args().skip(1).peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--channel" => { if let Some(v) = args.next() { channel = Box::leak(v.into_boxed_str()); } }
            "--target-os" => { if let Some(v) = args.next() { target_os = Box::leak(v.into_boxed_str()); } }
            "--target-family" => { args.next(); }
            _ => {}
        }
    }
    let json = format!(r#"{{
  "app_id": "dev.warp.Warp-{ch}",
  "logfile_name": "warp-{ch}.log",
  "server_config": {{
    "server_root_url": "https://app.warp.dev",
    "rtc_server_url": "wss://rtc.app.warp.dev/graphql/v2",
    "session_sharing_server_url": "wss://sessions.app.warp.dev",
    "firebase_auth_api_key": "AIzaSyBdy3O3S9hrdayLJxJ7mriBR4qgUaUygAs"
  }},
  "oz_config": {{
    "oz_root_url": "https://oz.warp.dev",
    "workload_audience_url": null
  }},
  "telemetry_config": {{
    "telemetry_file_name": "warp-{ch}-telemetry",
    "rudderstack_config": null
  }},
  "autoupdate_config": null,
  "crash_reporting_config": null,
  "mcp_static_config": null
}}"#, ch = channel);
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let _ = out.write_all(json.as_bytes());
}
