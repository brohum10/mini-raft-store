# Security Policy

Mini Raft Store accepts HTTP requests and persists replicated state. Request-validation bypasses, unsafe file handling, denial of service, data corruption, or violations that expose uncommitted data should be reported privately.

Use GitHub's **Security → Report a vulnerability** flow when available. Otherwise, contact the maintainer through the GitHub profile. Include a minimal failure schedule or reproduction, the affected version, and observed impact.

The current `main` branch is supported. This is an educational Raft subset, not an internet-facing database; production deployments require authentication, encryption, access control, snapshots, and a broader operational review.
