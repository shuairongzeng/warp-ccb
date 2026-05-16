//! Shared Memory Layer — Access Control
//!
//! Permission model:
//! - Global:  read = anyone, write = owner/admin, delete = owner/admin
//! - Project: read = same project_id, write = same project_id, delete = owner/admin
//! - Private: read/write/delete = owner only

use super::protocol::{MemoryEntry, MemoryEntryInput, MemoryScope};

/// Resolved role of the caller relative to an entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryRole {
    /// Can read project/global entries.
    Reader,
    /// Can read and write project/global entries.
    Writer,
    /// Can do anything, including force-overwrite and delete others' entries.
    Admin,
}

/// ACL check result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclCheck {
    Allow,
    Deny,
}

/// Compute the role of `caller` for a given entry or input.
pub fn resolve_role(
    caller: &str,
    scope: MemoryScope,
    project_id: Option<&str>,
    owner: Option<&str>,
    caller_project_id: Option<&str>,
) -> MemoryRole {
    let is_owner = owner.map_or(false, |o| o.eq_ignore_ascii_case(caller));

    // Admin override: if caller is an orchestrator or explicitly admin.
    // For now, only the bus itself ("system") is admin.
    if caller.eq_ignore_ascii_case("system") {
        return MemoryRole::Admin;
    }

    match scope {
        MemoryScope::Global => {
            if is_owner {
                MemoryRole::Writer
            } else {
                MemoryRole::Reader
            }
        }
        MemoryScope::Project => {
            let same_project = project_id.is_some()
                && caller_project_id.is_some()
                && project_id == caller_project_id;
            if is_owner || same_project {
                MemoryRole::Writer
            } else {
                MemoryRole::Reader
            }
        }
        MemoryScope::Private => {
            if is_owner {
                MemoryRole::Admin
            } else {
                // Private entries are invisible to non-owners.
                MemoryRole::Reader
            }
        }
    }
}

/// Check if caller can read an entry.
pub fn can_read(
    caller: &str,
    entry: &MemoryEntry,
    caller_project_id: Option<&str>,
) -> AclCheck {
    let role = resolve_role(caller, entry.scope, entry.project_id.as_deref(), Some(&entry.owner), caller_project_id);
    match entry.scope {
        MemoryScope::Private if role != MemoryRole::Admin => AclCheck::Deny,
        _ => AclCheck::Allow,
    }
}

/// Check if caller can write (create or update).
pub fn can_write(
    caller: &str,
    input: &MemoryEntryInput,
    existing_owner: Option<&str>,
    caller_project_id: Option<&str>,
) -> AclCheck {
    let role = resolve_role(caller, input.scope, input.project_id.as_deref(), existing_owner, caller_project_id);
    match role {
        MemoryRole::Admin | MemoryRole::Writer => AclCheck::Allow,
        MemoryRole::Reader => AclCheck::Deny,
    }
}

/// Check if caller can delete an entry.
pub fn can_delete(
    caller: &str,
    entry: &MemoryEntry,
    caller_project_id: Option<&str>,
) -> AclCheck {
    let role = resolve_role(caller, entry.scope, entry.project_id.as_deref(), Some(&entry.owner), caller_project_id);
    match role {
        MemoryRole::Admin => AclCheck::Allow,
        _ => AclCheck::Deny,
    }
}

/// Derive a project_id hash from a normalized working directory.
pub fn project_id_from_cwd(cwd: &str) -> String {
    use sha2::{Digest, Sha256};
    let normalized = cwd.to_lowercase().replace('\\', "/").replace("//", "/");
    let mut hasher = Sha256::new();
    hasher.update(normalized.as_bytes());
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(scope: MemoryScope, owner: &str, project_id: Option<&str>) -> MemoryEntry {
        MemoryEntry {
            id: "id".to_string(),
            scope,
            project_id: project_id.map(String::from),
            owner: owner.to_string(),
            key: "k".to_string(),
            content: "c".to_string(),
            content_type: "text".to_string(),
            version: 1,
            created_at_ms: 0,
            updated_at_ms: 0,
            ttl_secs: None,
            access_count: 0,
            last_accessed_ms: 0,
            size_bytes: 1,
            compressed: false,
            tags: vec![],
        }
    }

    #[test]
    fn test_global_read_anyone() {
        let entry = make_entry(MemoryScope::Global, "claude", None);
        assert_eq!(can_read("codex", &entry, None), AclCheck::Allow);
    }

    #[test]
    fn test_private_denies_other() {
        let entry = make_entry(MemoryScope::Private, "claude", Some("proj1"));
        assert_eq!(can_read("codex", &entry, Some("proj1")), AclCheck::Deny);
        assert_eq!(can_read("claude", &entry, Some("proj1")), AclCheck::Allow);
    }

    #[test]
    fn test_project_same_project_can_read() {
        let entry = make_entry(MemoryScope::Project, "claude", Some("proj1"));
        assert_eq!(can_read("codex", &entry, Some("proj1")), AclCheck::Allow);
        assert_eq!(can_read("codex", &entry, Some("proj2")), AclCheck::Allow); // reader role
    }

    #[test]
    fn test_delete_requires_admin() {
        let entry = make_entry(MemoryScope::Project, "claude", Some("proj1"));
        assert_eq!(can_delete("codex", &entry, Some("proj1")), AclCheck::Deny);
        assert_eq!(can_delete("claude", &entry, Some("proj1")), AclCheck::Allow);
    }

    #[test]
    fn test_project_id_hash() {
        let id1 = project_id_from_cwd("D:\\GitHub\\warp-ccb");
        let id2 = project_id_from_cwd("d:/github/warp-ccb");
        assert_eq!(id1, id2);
    }
}
