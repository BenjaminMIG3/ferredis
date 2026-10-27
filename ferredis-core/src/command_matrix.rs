//! Matrice des commandes Redis 8 non encore livrées (allowlist CI / erreurs explicites).
//!
//! **Politique anti-stub :** une entrée ici disparaît à la phase indiquée ; pas de `OK` no-op
//! permanent après la phase propriétaire (voir `PLAN.md`).

/// Phase plan (P1…P12) où la commande doit être implémentée.
pub fn deferred_phase(cmd: &str) -> Option<u16> {
    let u = cmd.to_ascii_uppercase();
    DEFERRED.iter().find(|(n, _)| *n == u).map(|(_, p)| *p)
}

/// Message d'erreur pour une commande connue mais pas encore implémentée.
pub fn deferred_error(cmd: &str) -> Option<String> {
    deferred_phase(cmd).map(|p| {
        format!(
            "ERR command '{}' is scheduled for Ferredis phase P{p} (see PLAN.md); not implemented yet",
            cmd.to_ascii_uppercase()
        )
    })
}

const DEFERRED: &[(&str, u16)] = &[
    // P10+
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p5_dump_family_registered() {
        for name in ["DUMP", "RESTORE", "RESTORE-ASKING", "MIGRATE", "WAITAOF"] {
            assert!(crate::command_registry::lookup(name).is_some(), "{name}");
            assert_eq!(deferred_phase(name), None, "{name}");
        }
    }

    #[test]
    fn p6_replication_registered() {
        for name in [
            "REPLICAOF", "SLAVEOF", "PSYNC", "SYNC", "REPLCONF", "ROLE", "WAIT", "FAILOVER",
            "READONLY", "READWRITE",
        ] {
            assert!(crate::command_registry::lookup(name).is_some(), "{name}");
            assert_eq!(deferred_phase(name), None, "{name}");
        }
    }

    #[test]
    fn p7_sentinel_registered() {
        assert!(crate::command_registry::lookup("SENTINEL").is_some());
        assert_eq!(deferred_phase("SENTINEL"), None);
    }

    #[test]
    fn p8_cluster_registered() {
        for name in ["CLUSTER", "ASKING"] {
            assert!(crate::command_registry::lookup(name).is_some(), "{name}");
            assert_eq!(deferred_phase(name), None, "{name}");
        }
    }

    #[test]
    fn p2_streams_are_registered() {
        for name in [
            "XGROUP", "XREADGROUP", "XACK", "XCLAIM", "XAUTOCLAIM", "XPENDING", "XINFO", "XDEL",
            "XTRIM", "XSETID", "XREVRANGE",
        ] {
            assert!(crate::command_registry::lookup(name).is_some(), "{name}");
            assert_eq!(deferred_phase(name), None, "{name}");
        }
    }

    #[test]
    fn p3_lua_acl_functions_are_registered() {
        for name in [
            "EVAL", "EVALSHA", "EVAL_RO", "EVALSHA_RO", "SCRIPT", "FUNCTION", "FCALL", "FCALL_RO",
            "ACL", "CLIENT", "MEMORY",
        ] {
            assert!(crate::command_registry::lookup(name).is_some(), "{name}");
            assert_eq!(deferred_phase(name), None, "{name}");
        }
    }

    #[test]
    fn p4_hello_client_registered() {
        for name in ["HELLO", "CLIENT"] {
            assert!(crate::command_registry::lookup(name).is_some(), "{name}");
            assert_eq!(deferred_phase(name), None, "{name}");
        }
    }

    #[test]
    fn p9_stack_commands_registered() {
        for name in [
            "JSON.SET", "JSON.GET", "JSON.DEL", "TS.CREATE", "TS.ADD", "TS.RANGE", "FT.CREATE",
            "FT.SEARCH", "FT.AGGREGATE", "FT._LIST", "MODULE",
        ] {
            assert!(crate::command_registry::lookup(name).is_some(), "{name}");
            assert_eq!(deferred_phase(name), None, "{name}");
        }
    }

    #[test]
    fn p10_prob_vector_registered() {
        for name in [
            "BF.RESERVE", "BF.ADD", "BF.EXISTS", "BF.MADD", "BF.INFO", "BF.SCANDUMP", "BF.LOADCHUNK",
            "CF.RESERVE", "CF.ADD", "CF.EXISTS", "CF.DEL", "CMS.INITBYDIM", "CMS.INCRBY", "CMS.QUERY",
            "TOPK.RESERVE", "TOPK.ADD", "TOPK.LIST", "TDIGEST.CREATE", "TDIGEST.ADD", "TDIGEST.QUANTILE",
            "VADD", "VSIM", "VCARD", "VREM", "VINFO",
        ] {
            assert!(crate::command_registry::lookup(name).is_some(), "{name}");
            assert_eq!(deferred_phase(name), None, "{name}");
        }
    }

    #[test]
    fn p11_hash_field_ttl_registered() {
        for name in [
            "HEXPIRE", "HPEXPIRE", "HEXPIREAT", "HPEXPIREAT", "HEXPIRETIME", "HPEXPIRETIME",
            "HTTL", "HPTTL", "HPERSIST", "HGETEX", "HSETEX", "HGETDEL",
        ] {
            assert!(crate::command_registry::lookup(name).is_some(), "{name}");
            assert_eq!(deferred_phase(name), None, "{name}");
        }
    }

    /// Aucune commande différée ne doit être servie par le registre (et inversement).
    #[test]
    fn deferred_and_registry_are_disjoint() {
        for (name, _) in DEFERRED {
            assert!(crate::command_registry::lookup(name).is_none(), "{name} est à la fois livré et différé");
        }
    }
}
