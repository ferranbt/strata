//! End-to-end tests for `#[config]`: they exercise the attribute on real structs
//! and observe behavior, rather than asserting on generated tokens.

use config_macro::config;

mod test {
    use super::*;

    /// `default_env` fields fall back to their env var when the key is missing,
    /// and `Config::default()` populates every field from those env-backed
    /// defaults. `#[config]` also makes the struct `Deserialize` on its own.
    #[config]
    #[derive(Debug, PartialEq)]
    struct WithEnv {
        #[config(default_env = "CONFIG_MACRO_TEST_KEY")]
        key: String,
        #[config(default_env = "CONFIG_MACRO_TEST_OTHER")]
        other: String,
    }

    #[test]
    fn default_reads_env() {
        unsafe { std::env::set_var("CONFIG_MACRO_TEST_KEY", "from-env") };
        unsafe { std::env::remove_var("CONFIG_MACRO_TEST_OTHER") };
        assert_eq!(
            WithEnv::default(),
            WithEnv { key: "from-env".into(), other: String::new() }
        );
        unsafe { std::env::remove_var("CONFIG_MACRO_TEST_KEY") };
    }

    /// A missing key in the payload falls back to the env var on deserialization.
    #[config]
    #[derive(Debug)]
    struct Missing {
        #[config(default_env = "CONFIG_MACRO_TEST_MISSING")]
        missing: String,
    }

    #[test]
    fn deserialize_missing_key_falls_back_to_env() {
        unsafe { std::env::set_var("CONFIG_MACRO_TEST_MISSING", "env-value") };
        let config: Missing = serde_json::from_str("{}").unwrap();
        assert_eq!(config.missing, "env-value");
        unsafe { std::env::remove_var("CONFIG_MACRO_TEST_MISSING") };
    }

    /// An explicit value in the payload wins; the env default is not consulted.
    #[config]
    #[derive(Debug)]
    struct Present {
        #[config(default_env = "CONFIG_MACRO_TEST_PRESENT")]
        present: String,
    }

    #[test]
    fn deserialize_present_key_wins_over_env() {
        unsafe { std::env::set_var("CONFIG_MACRO_TEST_PRESENT", "env-value") };
        let config: Present = serde_json::from_str(r#"{"present":"explicit"}"#).unwrap();
        assert_eq!(config.present, "explicit");
        unsafe { std::env::remove_var("CONFIG_MACRO_TEST_PRESENT") };
    }

    /// When the env var is unset, the env-backed default is the type's `Default`.
    #[config]
    #[derive(Debug)]
    struct Unset {
        #[config(default_env = "CONFIG_MACRO_TEST_UNSET")]
        unset: String,
    }

    #[test]
    fn default_when_env_unset_is_type_default() {
        unsafe { std::env::remove_var("CONFIG_MACRO_TEST_UNSET") };
        assert_eq!(Unset::default().unset, String::new());
    }

    /// A non-`String` field parses its env var into the field type.
    #[config]
    #[derive(Debug)]
    struct Typed {
        #[config(default_env = "CONFIG_MACRO_TEST_PORT")]
        port: u16,
    }

    #[test]
    fn env_var_is_parsed_into_field_type() {
        unsafe { std::env::set_var("CONFIG_MACRO_TEST_PORT", "8123") };
        let config: Typed = serde_json::from_str("{}").unwrap();
        assert_eq!(config.port, 8123);
        unsafe { std::env::remove_var("CONFIG_MACRO_TEST_PORT") };
    }
}
