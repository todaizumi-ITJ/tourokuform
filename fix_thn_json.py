#!/usr/bin/env python3
"""
fix_thn_json.py — 登録JSON 機械チェック＋自動修正ツール

使い方:
  python fix_thn_json.py input.json output.json

処理内容:
  1. rights_holder が人名（漢字2〜6文字、法人キーワードなし）→ "" に修正
  2. timestamp 形式を YYYY/MM/DD HH:mm:ss に正規化
  3. ip_address のスペース除去、空なら「IP要確認」追記
  4. ipn_id が数字のみ → "" に修正
  5. address が10文字未満 → 「住所要確認」追記
  6. contract_name が空 → addressから法人名を補完試行
  7. 同一 ip_address + timestamp の重複行を検出 → 2件目以降除去

修正した場合は memo に「修正: （理由）」を追記。
処理後サマリーを標準出力に表示。
"""

import json
import re
import sys
import unicodedata

# ============================================================
# 判定用定数
# ============================================================
CORP_KEYWORDS = [
    '株式会社', '有限会社', '合同会社', '合資会社', '合名会社',
    '一般社団法人', '一般財団法人', '公益社団法人', '公益財団法人',
    '特定非営利活動法人', 'NPO法人',
    '医療法人', '社会福祉法人', '学校法人', '宗教法人',
    '独立行政法人', '国立大学法人',
    'LLC', 'Inc', 'Corp', 'Ltd', 'Co.',
]

IP_RE = re.compile(r'^\d{1,3}(\.\d{1,3}){3}$')

# timestamp正規化用パターン
TS_PATTERNS = [
    # YYYY/MM/DD HH:mm:ss（正規形）
    re.compile(r'^(\d{4})/(\d{1,2})/(\d{1,2})\s+(\d{1,2}):(\d{1,2}):(\d{1,2})$'),
    # YYYY-MM-DD HH:mm:ss
    re.compile(r'^(\d{4})-(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{1,2}):(\d{1,2})$'),
    # YYYY/MM/DD HH:mm（秒なし）
    re.compile(r'^(\d{4})/(\d{1,2})/(\d{1,2})\s+(\d{1,2}):(\d{1,2})$'),
    # YYYY-MM-DD HH:mm（秒なし）
    re.compile(r'^(\d{4})-(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{1,2})$'),
    # YYYYMMDD HH:mm:ss
    re.compile(r'^(\d{4})(\d{2})(\d{2})\s+(\d{1,2}):(\d{1,2}):(\d{1,2})$'),
]


# ============================================================
# ユーティリティ
# ============================================================
def normalize_str(s):
    """NFKC正規化＋前後空白除去"""
    if not s:
        return ''
    return unicodedata.normalize('NFKC', s).strip()


def append_memo(record, msg):
    """memoフィールドに追記"""
    old = record.get('memo', '') or ''
    sep = '；' if old else ''
    record['memo'] = old + sep + msg


def is_person_name(text):
    """rights_holderが人名かどうか判定（漢字2〜6文字、法人キーワードなし）"""
    if not text:
        return False
    t = normalize_str(text)
    if len(t) < 2 or len(t) > 10:
        return False
    # 法人キーワードが含まれていたら人名ではない
    for kw in CORP_KEYWORDS:
        if kw in t:
            return False
    # 全文字が漢字・ひらがな・カタカナ・スペースのみ → 人名の疑い
    cleaned = t.replace(' ', '').replace('　', '')
    if re.match(r'^[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff]+$', cleaned):
        return True
    return False


def normalize_timestamp(ts):
    """timestampを YYYY/MM/DD HH:mm:ss に正規化。不正なら空文字を返す"""
    if not ts:
        return ''
    t = normalize_str(ts)
    for pat in TS_PATTERNS:
        m = pat.match(t)
        if m:
            groups = m.groups()
            y = groups[0]
            mo = groups[1].zfill(2)
            d = groups[2].zfill(2)
            h = groups[3].zfill(2)
            mi = groups[4].zfill(2)
            s = groups[5].zfill(2) if len(groups) >= 6 else '00'
            return f'{y}/{mo}/{d} {h}:{mi}:{s}'
    return ''


def extract_corp_from_address(addr):
    """住所文字列から法人名を抽出する試み"""
    if not addr:
        return ''
    for kw in CORP_KEYWORDS:
        idx = addr.find(kw)
        if idx >= 0:
            # キーワードの前後を含めて法人名を抽出
            # キーワードが先頭にある場合: 株式会社〇〇
            # キーワードが途中にある場合: 〇〇株式会社
            # 簡易的に、キーワード前後の非空白文字を取得
            before = addr[:idx].split()[-1] if addr[:idx].strip() else ''
            after_text = addr[idx + len(kw):]
            after = after_text.split()[0] if after_text.strip() else ''
            if kw.startswith(('株式', '有限', '合同', '合資', '合名')):
                return kw + after
            else:
                return before + kw
    return ''


# ============================================================
# メイン処理
# ============================================================
def process_record(r, stats):
    """1レコードを処理。statsに集計を記録"""
    modified = False

    # --- rights_holder 人名チェック ---
    rh = r.get('rights_holder', '')
    if rh and is_person_name(rh):
        r['rights_holder'] = ''
        append_memo(r, f'修正: rights_holder "{rh}" は人名の疑いがあるため空に変更')
        stats['rights_holder_fixed'] += 1
        modified = True

    # --- timestamp 正規化 ---
    ts = r.get('timestamp', '')
    if ts:
        normalized = normalize_timestamp(ts)
        if normalized and normalized != normalize_str(ts):
            r['timestamp'] = normalized
            append_memo(r, f'修正: timestamp "{ts}" → "{normalized}"')
            stats['timestamp_fixed'] += 1
            modified = True
        elif not normalized:
            r['timestamp'] = ''
            append_memo(r, f'修正: timestamp "{ts}" は形式不正のため空に変更')
            stats['timestamp_fixed'] += 1
            modified = True

    # --- ip_address スペース除去 ---
    ip = r.get('ip_address', '')
    if ip:
        cleaned_ip = ip.replace(' ', '').replace('　', '')
        if cleaned_ip != ip:
            r['ip_address'] = cleaned_ip
            append_memo(r, f'修正: ip_address のスペースを除去')
            stats['ip_fixed'] += 1
            modified = True
        if not IP_RE.match(cleaned_ip):
            append_memo(r, 'IP要確認: ip_address の形式が不正')
            stats['ip_warning'] += 1
            modified = True
    elif r.get('source_type') == 'provider':
        append_memo(r, 'IP要確認: ip_address が空')
        stats['ip_warning'] += 1
        modified = True

    # --- ipn_id 数字のみチェック ---
    ipn = r.get('ipn_id', '')
    if ipn and re.match(r'^\d+$', ipn.strip()):
        old_ipn = ipn
        r['ipn_id'] = ''
        append_memo(r, f'修正: ipn_id "{old_ipn}" は数字のみのため空に変更')
        stats['ipn_fixed'] += 1
        modified = True

    # --- address 短すぎ ---
    addr = r.get('address', '')
    if addr and len(addr.strip()) < 10:
        append_memo(r, '住所要確認: address が10文字未満')
        stats['address_warning'] += 1
        modified = True

    # --- contract_name 空 → 住所から法人名補完 ---
    name = r.get('contract_name', '')
    if not name or not name.strip():
        corp = extract_corp_from_address(addr)
        if corp:
            r['contract_name'] = corp
            append_memo(r, f'修正: contract_name を address から補完 → "{corp}"')
            stats['name_complemented'] += 1
            modified = True
        else:
            stats['name_empty'] += 1

    return modified


def remove_duplicates(records, stats):
    """同一 ip_address + timestamp の重複行を除去（2件目以降を削除）"""
    seen = set()
    result = []
    for r in records:
        ip = r.get('ip_address', '')
        ts = r.get('timestamp', '')
        if ip and ts:
            key = f'{ip}|{ts}'
            if key in seen:
                stats['duplicates_removed'] += 1
                continue
            seen.add(key)
        result.append(r)
    return result


def main():
    if len(sys.argv) < 3:
        print('使い方: python fix_thn_json.py input.json output.json')
        print('')
        print('  input.json   登録.html からエクスポートしたJSONファイル')
        print('  output.json  修正済みJSONの出力先')
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # 読み込み
    with open(input_path, 'r', encoding='utf-8') as f:
        records = json.load(f)

    if not isinstance(records, list):
        print('エラー: JSONファイルは配列形式である必要があります')
        sys.exit(1)

    total = len(records)
    print(f'入力: {total}件')
    print(f'─────────────────────────────────')

    # 統計
    stats = {
        'rights_holder_fixed': 0,
        'timestamp_fixed': 0,
        'ip_fixed': 0,
        'ip_warning': 0,
        'ipn_fixed': 0,
        'address_warning': 0,
        'name_complemented': 0,
        'name_empty': 0,
        'duplicates_removed': 0,
    }

    # 各レコード処理
    modified_count = 0
    for r in records:
        if process_record(r, stats):
            modified_count += 1

    # 重複除去
    records = remove_duplicates(records, stats)

    # 出力
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    # サマリー表示
    print(f'')
    print(f'【修正サマリー】')
    print(f'  rights_holder 人名修正:  {stats["rights_holder_fixed"]}件')
    print(f'  timestamp 正規化:        {stats["timestamp_fixed"]}件')
    print(f'  ip_address 修正:         {stats["ip_fixed"]}件')
    print(f'  ip_address 要確認:       {stats["ip_warning"]}件')
    print(f'  ipn_id 数字のみ修正:     {stats["ipn_fixed"]}件')
    print(f'  address 短すぎ警告:      {stats["address_warning"]}件')
    print(f'  contract_name 法人名補完: {stats["name_complemented"]}件')
    print(f'  contract_name 空のまま:  {stats["name_empty"]}件')
    print(f'  重複除去:                {stats["duplicates_removed"]}件')
    print(f'─────────────────────────────────')
    print(f'  修正対象レコード:        {modified_count}/{total}件')
    print(f'  出力件数:                {len(records)}件')
    print(f'')

    warnings = stats['ip_warning'] + stats['address_warning'] + stats['name_empty']
    if warnings > 0:
        print(f'⚠️  要確認が {warnings}件 あります。目視チェックを推奨します。')
    else:
        print(f'✅ 問題なし。出力ファイルをそのまま登録.html にインポートできます。')

    print(f'出力: {output_path}')


if __name__ == '__main__':
    main()
