from datetime import datetime, timedelta
def to_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None

def fmt_round(val, decimals=0):
    val = to_float(val)
    if val is None:
        return "N/A"
    return round(val, decimals)
def get_detail(details, key):
    if isinstance(details, dict):
        return details.get(key)
    if isinstance(details, list) and len(details) > 0 and isinstance(details[0], dict):
        return details[0].get(key)
    return None

def generate_report(items):
    fecha_hoy = (datetime.now() - timedelta(days=2)).strftime("%d/%m/%Y")

    flags = {
        "GLOBAL": "🌐", "MLA": "🇦🇷", "MLB": "🇧🇷", "MLM": "🇲🇽",
        "MLC": "🇨🇱", "MCO": "🇨🇴", "MLU": "🇺🇾", "MPE": "🇵🇪",
        "MEC": "🇪🇨", "ALL": "🌎"
    }

    grouped = {}

    for item in items:
        data = item.get("json", {})

        tp = str(data.get("touchpoint", "N/A"))
        site = str(data.get("site", "N/A")).upper()
        cat = str(data.get("category", "N/A"))
        rule = str(data.get("rule_type", "N/A"))

        value = data.get("value", 0)
        expected = data.get("expected")
        delta_pct = data.get("delta_pct")
        details = data.get("details", {})

        key = f"{tp}|{site}"

        if key not in grouped:
            grouped[key] = {
                "tp": tp,
                "site": site,
                "cat": cat,
                "rules": [],
                "value": value,
                "expected": expected,
                "delta_pct": delta_pct,
                "details": details
            }

        if rule not in grouped[key]["rules"]:
            grouped[key]["rules"].append(rule)

    sections = {
        "warning": [],
        "prevention": [],
        "trend": [],
        "success": []
    }

    for info in grouped.values():
        formatted_rules = []

        for r in info["rules"]:
            if "ZERO_DROP" in r:
                formatted_rules.append(f"🚨 `{r}`")
            elif "WOW" in r:
                formatted_rules.append(f"⚠️ `{r}`")
            elif "UP" in r:
                formatted_rules.append(f"📈 `{r}`")
            elif "DOWN" in r:
                formatted_rules.append(f"📉 `{r}`")
            else:
                formatted_rules.append(f"`{r}`")

        rules_str = ", ".join(formatted_rules)
        flag = flags.get(info["site"], "📍")

        # --- Valor contextual según categoría ---
        cat = info["cat"]
        val_str = ""

        if "ZERO_DROP" in rules_str:
            val_str = f"Val Prev: *{fmt_round(info['expected'], 0)}*"

        elif "WOW" in rules_str:
            val_str = (
                f"Val LW: *{fmt_round(info['expected'], 0)}* | "
                f"Δ *{fmt_round(info['delta_pct'], 2)}%*"
            )

        elif cat == "prevention":
            z = get_detail(info["details"], "z_score")
            val_str = (
                f"Val Exp: *{fmt_round(info['expected'], 0)}* | "
                f"Z: *{fmt_round(z, 2)}*"
            )


        elif cat == "trend":
            val_str = f"DELTA_PCT% (L3D): *{fmt_round(info['delta_pct'], 2)}%*"

        else:
            val_str = f"Val: *{fmt_round(info['value'], 0)}*"


        line = f"• {flag} *{info['tp']}* | {rules_str} | {val_str}"
        if cat in sections:
            sections[cat].append(line)

    res = [f"📊 *REPORTE DIARIO DE TOUCHPOINTS - {fecha_hoy}* 📊"]

    mapping = [
        ("warning", "🔴 CRITICAL"),
        ("prevention", "🟡 PREVENTION"),
        ("trend", "🔵 TRENDS"),
        ("success", "🚀 SUCCESS")
    ]

    for key, label in mapping:
        if sections[key]:
            res.append("")
            res.append(label)
            res.extend(sections[key])

    final_text = "\n".join(res).strip()

    return [{"json": {"slack_message": final_text}}]

return generate_report(items)
