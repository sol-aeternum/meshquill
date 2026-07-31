# Security policy

## Reporting a vulnerability

After publication, use GitHub private vulnerability reporting from the repository's **Security**
tab. If that form is unavailable, open a content-free issue requesting a private reporting channel
and disclose no vulnerability details there. Do not include production
credentials, private keys, message contents, or personal location data; use synthetic reproductions.

Maintainers aim to acknowledge a complete report within three business days, provide an initial
severity assessment within seven days, and give a remediation or coordination update at least every
14 days until closure. These are response targets, not a warranty. Coordinated disclosure timing is
agreed with the reporter after affected supported versions and mitigations are known.

## Supported versions

`0.1.0-rc.3` is the current release-candidate line. Published availability is recorded in the
[live repository status](https://github.com/sol-aeternum/meshquill/blob/main/STATUS.md). Until that
status records a public RC, source receives best-effort fixes.
After publication, the newest RC is supported for security fixes; older pre-release lines are
unsupported unless explicitly listed here.

## Security boundaries

- Device, radio, serial, TCP, MQTT, imported files and hook output are untrusted.
- Configured Python hooks are trusted local code. Subprocess isolation is not a sandbox.
- Meshquill does not replace or strengthen MeshCore's radio-layer cryptography.
- MQTT is optional and may add a LAN or internet dependency to an otherwise off-grid system.

See `docs/threat-model.md` for controls and exclusions.
