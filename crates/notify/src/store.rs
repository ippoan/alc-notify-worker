//! 文書のファイルの置き場 (Worker では R2 の binding `NOTIFY_R2`、テストではモック) と、取り出しの口の `Content-Disposition`
//! (ippoan/alc-carins-worker の `store.rs` の `FileStore`・`content_disposition` の写し。閲覧の口だけなので読みの `get` だけを持つ)。

use std::sync::Arc;

use futures_util::future::BoxFuture;

/// R2 の段の失敗。持つのは段の名前だけ (キーやランタイムのエラー文を持たない)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreError(pub &'static str);

/// axum の state と handler が要るので `Send + Sync` で `Send` な future を返す
pub trait FileStore: Send + Sync {
    /// 無いキーも失敗 (rust の download と同じく 500 にする)
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Vec<u8>, StoreError>>;
}

pub type SharedStore = Arc<dyn FileStore>;

/// `Content-Disposition` (RFC 6266)。`disposition` は `inline` か `attachment`。**ASCII だけ**で組む: `filename*` に
/// UTF-8 の percent-encoding (RFC 5987)、`filename` に ASCII の代替 (非 ASCII・制御文字・`"`・`\` を `_` に)。
///
/// **rust と違う**: rust は `filename="<名前の " を _ に>"` で日本語を生の UTF-8 のまま返す。Workers は非 ASCII のヘッダ値を、
/// 値 (= ファイル名) 入りの警告にして出すので、ファイル名がログに出る経路を作らないよう ASCII にする
/// (ippoan/alc-carins-worker・ippoan/alc-trouble-worker と同じ)。画面 (nuxt-notify) はこのヘッダを読まず、名前は JSON から取る。
pub fn content_disposition(disposition: &str, filename: &str) -> String {
    let fallback: String = filename
        .chars()
        .map(|c| match c {
            ' '..='~' if c != '"' && c != '\\' => c,
            _ => '_',
        })
        .collect();
    let mut encoded = String::new();
    for b in filename.bytes() {
        if b.is_ascii_alphanumeric() || b"!#$&+-.^_`|~".contains(&b) {
            encoded.push(b as char);
        } else {
            encoded.push_str(&format!("%{b:02X}"));
        }
    }
    format!("{disposition}; filename*=UTF-8''{encoded}; filename=\"{fallback}\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disposition_is_ascii_with_filename_star() {
        assert_eq!(
            content_disposition("attachment", "点呼 \"新\"\\a\t.pdf"),
            "attachment; filename*=UTF-8''%E7%82%B9%E5%91%BC%20%22%E6%96%B0%22%5Ca%09.pdf; \
             filename=\"__ ____a_.pdf\""
        );
        assert_eq!(
            content_disposition("inline", "3164_001(1).pdf"),
            "inline; filename*=UTF-8''3164_001%281%29.pdf; filename=\"3164_001(1).pdf\""
        );
        assert!(content_disposition("inline", "配車🚚.jpg").is_ascii());
    }
}
