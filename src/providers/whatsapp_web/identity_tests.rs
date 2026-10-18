use super::*;

#[test]
fn identity_is_unknown_until_the_session_reports_an_account() {
    let identity = LinkedIdentity::default();
    assert_eq!(identity.current(), None);
    assert!(identity.observe(None));
    assert!(identity.observe(Some("  ")));
    assert_eq!(identity.current(), None);
}

#[test]
fn the_first_account_binds_and_the_same_account_stays_valid() {
    let identity = LinkedIdentity::default();
    assert!(identity.observe(Some("15550001111@s.whatsapp.net")));
    assert_eq!(
        identity.current().as_deref(),
        Some("15550001111@s.whatsapp.net")
    );
    assert!(identity.observe(Some("15550001111@s.whatsapp.net")));
    assert!(identity.observe(None));
    assert!(!identity.is_conflicted());
}

#[test]
fn a_different_account_poisons_the_identity_for_good() {
    let identity = LinkedIdentity::default();
    assert!(identity.observe(Some("15550001111@s.whatsapp.net")));
    assert!(!identity.observe(Some("15550002222@s.whatsapp.net")));
    assert!(identity.is_conflicted());
    assert_eq!(identity.current(), None);
    // Returning to the first account does not heal it.
    assert!(!identity.observe(Some("15550001111@s.whatsapp.net")));
    assert!(!identity.observe(None));
    assert_eq!(identity.current(), None);
}
