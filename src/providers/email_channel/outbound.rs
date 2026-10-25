//! SMTP credentials, message construction, and delivery.

use super::*;
use lettre::message::{Attachment, MultiPart, SinglePart, header::ContentType};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Message, SmtpTransport, Transport};
use tracing::info;

impl EmailChannel {
    fn create_smtp_transport(&self) -> Result<SmtpTransport> {
        let creds = Credentials::new(self.config.username.clone(), self.config.password.clone());
        let transport = if self.config.smtp_tls {
            SmtpTransport::relay(&self.config.smtp_host)?
                .port(self.config.smtp_port)
                .credentials(creds)
                .build()
        } else {
            SmtpTransport::builder_dangerous(&self.config.smtp_host)
                .port(self.config.smtp_port)
                .credentials(creds)
                .build()
        };
        Ok(transport)
    }

    /// This channel's SMTP account as per-call [`SmtpCredentials`], for the
    /// async [`crate::providers::mail::MailSender`] seam.
    ///
    /// `smtp_tls` maps to implicit TLS ([`SmtpSecurity::Ssl`]) and its absence to
    /// a plaintext connection ([`SmtpSecurity::None`]) — the same two transports
    /// the blocking [`Self::send_message`] path builds. `from_address` is used
    /// as given, so a `Name <addr>` form keeps its display name.
    pub fn smtp_credentials(&self) -> SmtpCredentials {
        SmtpCredentials {
            host: self.config.smtp_host.clone(),
            port: self.config.smtp_port,
            security: if self.config.smtp_tls {
                SmtpSecurity::Ssl
            } else {
                SmtpSecurity::None
            },
            username: self.config.username.clone(),
            password: MailSecret::new(self.config.password.clone()),
            from_name: String::new(),
            from_email: self.config.from_address.clone(),
        }
    }

    /// This channel's IMAP login as per-call [`ImapCredentials`], for the
    /// [`MailReceiver`](crate::providers::mail::MailReceiver) seam.
    pub fn imap_credentials(&self) -> ImapCredentials {
        ImapCredentials {
            host: self.config.imap_host.clone(),
            port: self.config.imap_port,
            username: self.config.username.clone(),
            password: MailSecret::new(self.config.password.clone()),
        }
    }

    pub fn send_message(&self, email: Message) -> Result<()> {
        let transport = self.create_smtp_transport()?;
        transport.send(&email)?;
        info!("Email sent");
        Ok(())
    }

    pub fn build_plain_message(
        &self,
        recipient: &str,
        subject: &str,
        body: &str,
    ) -> Result<Message> {
        Message::builder()
            .from(self.config.from_address.parse()?)
            .to(recipient.parse()?)
            .subject(subject)
            .singlepart(SinglePart::plain(body.to_string()))
            .map_err(Into::into)
    }

    pub fn build_message_with_attachment(
        &self,
        recipient: &str,
        subject: &str,
        body: &str,
        attachment_name: &str,
        content_type: ContentType,
        attachment_bytes: Vec<u8>,
    ) -> Result<Message> {
        let attachment =
            Attachment::new(attachment_name.to_string()).body(attachment_bytes, content_type);
        Message::builder()
            .from(self.config.from_address.parse()?)
            .to(recipient.parse()?)
            .subject(subject)
            .multipart(
                MultiPart::mixed()
                    .singlepart(SinglePart::plain(body.to_string()))
                    .singlepart(attachment),
            )
            .map_err(Into::into)
    }
}
