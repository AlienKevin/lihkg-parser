use html5ever::tree_builder::TreeSink;
use lazy_static::lazy_static;
use regex::Regex;
use scraper::node::Node;
use scraper::{ElementRef, Html, Selector};
use serde_json::Value;
use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use tar::Archive;
use xz2::read::XzDecoder;

lazy_static! {
    static ref CJK_REGEX: Regex = Regex::new(r"\p{Unified_Ideograph}").unwrap();
    static ref WORD_REGEX: Regex =
        Regex::new(r"[[:alnum:]]+|\p{Unified_Ideograph}|\p{Punct}+").unwrap();
    static ref PUNCS: HashSet<char> = {
        SHARED_PUNCS
            .union(&ENGLISH_PUNCS)
            .copied()
            .collect::<HashSet<char>>()
            .union(&CHINESE_PUNCS)
            .copied()
            .collect()
    };
    static ref SHARED_PUNCS: HashSet<char> =
        HashSet::from(['@', '#', '$', '%', '^', '&', '*', '·', '…', '‥', '—', '～']);
    static ref ENGLISH_PUNCS: HashSet<char> = {
        HashSet::from([
            '~', '`', '!', '(', ')', '-', '_', '{', '}', '[', ']', '|', '\\', ':', ';', '"', '\'',
            '<', '>', ',', '.', '?', '/',
        ])
    };
    static ref CHINESE_PUNCS: HashSet<char> = {
        HashSet::from([
            '！', '：', '；', '“', '”', '‘', '’', '【', '】', '（', '）', '「', '」', '﹁', '﹂',
            '『', '』', '《', '》', '？', '，', '。', '、', '／', '＋', '〈', '〉', '︿', '﹀',
            '［', '］', '‧',
        ])
    };
}

fn filter_irrelevant_chars(text: &str) -> String {
    text.chars()
        .filter(|c| CJK_REGEX.is_match(&c.to_string()) || is_punc(*c) || c.is_ascii_alphanumeric())
        .collect()
}

fn is_punc(c: char) -> bool {
    PUNCS.contains(&c)
}

fn count_matching_chars(text: &str, regex: &Regex) -> usize {
    text.chars()
        .filter(|c| regex.is_match(&c.to_string()))
        .count()
}

fn is_valid_para(para: &str) -> bool {
    if para.is_empty() {
        return false; // no content
    }
    if para == "此回覆已被刪除" {
        return false;
    }
    if para.contains("分享自 LIHKG 討論區") {
        return false;
    }
    let len = para.chars().count();
    if len < 5 || len > 20 {
        return false; // length < 5 or length > 20
    }
    if para.contains("http://") || para.contains("https://") {
        return false; // includes URL
    }

    let english_words_re = Regex::new(r"^[A-Za-z ]+$").unwrap();
    if english_words_re.is_match(para) {
        return false; // only English words
    }

    let date_re = Regex::new(r"^\d{4}.\d{2}.\d{2}$").unwrap();
    if date_re.is_match(para) {
        return false; // date
    }

    let time_re = Regex::new(r"^\d{2}:\d{2}:\d{2}$").unwrap();
    if time_re.is_match(para) {
        return false; // time
    }

    let unique_chars: std::collections::HashSet<char> = para.chars().collect();
    if unique_chars.len() * 5 < para.len() {
        return false; // too many repeated characters
    }

    true
}

fn convert_html_to_text(html: &str) -> String {
    let mut document = Html::parse_fragment(html);

    // Remove blockquote
    let blockquote_selector = Selector::parse("blockquote").unwrap();
    let node_ids: Vec<_> = document
        .select(&blockquote_selector)
        .map(|x| x.id())
        .collect();
    for id in node_ids {
        document.remove_from_parent(&id);
    }

    // Convert to text
    document.root_element().text().collect()
}

fn process_line(line: &str, result: &mut String) -> Result<(), serde_json::Error> {
    let line = line.split("\t").nth(2).unwrap();
    let obj: Value = serde_json::from_str(line)?;

    if obj["success"].as_i64() == Some(1) {
        if let Some(item_data) = obj["response"]["item_data"].as_array() {
            for item in item_data {
                if let Some(msg) = item["msg"].as_str() {
                    let text = convert_html_to_text(msg);
                    let paras = text.split("\n");
                    for para in paras {
                        let para = para.trim();
                        if is_valid_para(para) {
                            let num_cjk = count_matching_chars(para, &CJK_REGEX);
                            let num_total = para.chars().count();
                            if num_cjk >= 5 && num_cjk > ((num_total as f32 * 0.8).round() as usize)
                            {
                                let para = filter_irrelevant_chars(para);
                                result.push_str(&para);
                                result.push('\n');
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tar_xz = File::open("./data/lihkg-1-1750000-csv.tar.xz")?;
    let tar = XzDecoder::new(BufReader::new(tar_xz));
    let mut archive = Archive::new(tar);

    // Create or open the output file
    let output_file = Arc::new(Mutex::new(File::create("sentences.txt")?));

    let mut handles = vec![];

    for file in archive.entries()? {
        let file = file.unwrap();
        let mut lines = Vec::new();
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line.unwrap();
            lines.push(line);
            if lines.len() >= 1000 {
                let output_file = Arc::clone(&output_file);
                let lines_to_process = std::mem::replace(&mut lines, Vec::new()); // Replace the current lines vector with a new one

                // Spawn a new thread for each batch
                let handle = std::thread::spawn(move || {
                    let mut result = String::new();

                    for line in lines_to_process {
                        process_line(&line, &mut result).unwrap();
                    }

                    // Write to the output file
                    output_file
                        .lock()
                        .unwrap()
                        .write_all(result.as_bytes())
                        .unwrap();
                });
                handles.push(handle);
            }
        }
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    Ok(())
}
