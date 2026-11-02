//! Lua EVAL / SCRIPT / FUNCTION — sémantique Redis OSS.

use super::harness::BurnCtx;
use ferredis_core::resp::RespValue;

pub async fn run(ctx: &mut BurnCtx) {
    const S: &str = "scripting";
    let mut c = ctx.connect().await;

    let k = ctx.unique("lua");
    ctx.expect_ok(
        S,
        "eval_set",
        &mut c,
        &[
            "EVAL",
            "return redis.call('SET', KEYS[1], ARGV[1])",
            "1",
            &k,
            "from-lua",
        ],
    )
    .await;
    ctx.expect_bulk(S, "eval_get_check", &mut c, &["GET", &k], "from-lua")
        .await;

    ctx.expect_int(
        S,
        "eval_return_int",
        &mut c,
        &["EVAL", "return 42", "0"],
        42,
    )
    .await;
    ctx.expect_bulk(
        S,
        "eval_return_bulk",
        &mut c,
        &["EVAL", "return 'hi'", "0"],
        "hi",
    )
    .await;
    ctx.expect_eq(
        S,
        "eval_return_table",
        &mut c,
        &["EVAL", "return {1,2,3}", "0"],
        super::harness::arr(vec![
            ferredis_core::resp::RespValue::Integer(1),
            ferredis_core::resp::RespValue::Integer(2),
            ferredis_core::resp::RespValue::Integer(3),
        ]),
    )
    .await;

    // cjson
    ctx.expect_bulk(
        S,
        "eval_cjson",
        &mut c,
        &[
            "EVAL",
            "local cjson = cjson; return cjson.encode({a=1})",
            "0",
        ],
        "{\"a\":1}",
    )
    .await;

    // SCRIPT LOAD / EXISTS / EVALSHA / FLUSH
    let script = "return KEYS[1]";
    let sha = {
        let t0 = std::time::Instant::now();
        match ctx.cmd(&mut c, &["SCRIPT", "LOAD", script]).await {
            Ok(RespValue::BulkString(Some(b))) => {
                let s = String::from_utf8_lossy(&b).into_owned();
                ctx.check(S, "script_load", t0.elapsed(), s.len() == 40, format!("SHA1 len=40, got {s}"));
                s
            }
            Ok(other) => {
                ctx.check(
                    S,
                    "script_load",
                    t0.elapsed(),
                    false,
                    format!("SCRIPT LOAD → bulk sha, got {other:?}"),
                );
                String::new()
            }
            Err(e) => {
                ctx.err_case(S, "script_load", t0.elapsed(), e);
                String::new()
            }
        }
    };
    if !sha.is_empty() {
        ctx.expect_eq(
            S,
            "script_exists",
            &mut c,
            &["SCRIPT", "EXISTS", &sha],
            super::harness::arr(vec![ferredis_core::resp::RespValue::Integer(1)]),
        )
        .await;
        ctx.expect_bulk(
            S,
            "evalsha",
            &mut c,
            &["EVALSHA", &sha, "1", "mykey"],
            "mykey",
        )
        .await;
    }

    // EVAL_RO ne doit pas écrire
    ctx.expect_error_contains(
        S,
        "eval_ro_write_denied",
        &mut c,
        &[
            "EVAL_RO",
            "return redis.call('SET', KEYS[1], 'x')",
            "1",
            &ctx.unique("ro"),
        ],
        "Write commands are not allowed",
    )
    .await;

    ctx.expect_ok(S, "script_flush", &mut c, &["SCRIPT", "FLUSH"]).await;

    // FUNCTION / FCALL (Redis 7+)
    let lib = r#"#!lua name=burnlib
redis.register_function('echo', function(keys, args) return args[1] end)
"#;
    ctx.expect_pred(
        S,
        "function_load",
        &mut c,
        &["FUNCTION", "LOAD", "REPLACE", lib],
        |v| match v {
            RespValue::BulkString(Some(b)) if b.as_ref() == b"burnlib" => Ok(()),
            RespValue::SimpleString(b) if b.as_ref() == b"burnlib" => Ok(()),
            other => Err(format!("FUNCTION LOAD → burnlib, got {other:?}")),
        },
    )
    .await;
    ctx.expect_bulk(
        S,
        "fcall",
        &mut c,
        &["FCALL", "echo", "0", "hello"],
        "hello",
    )
    .await;
    ctx.expect_ok(S, "function_flush", &mut c, &["FUNCTION", "FLUSH"])
        .await;
}
