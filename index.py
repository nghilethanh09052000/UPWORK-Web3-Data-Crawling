import json
import os
from urllib.parse import urlparse


BASE_DIR = "/Users/nghilethanh/Project/UPWORK-Scrape-Interactive-Chart-Data /project/"

PROTOCOL_GOVERNANCE_FILE = os.path.join(BASE_DIR, "protocol_governance.json")
PROTOCOL_DETAILS_FILE = os.path.join(BASE_DIR, "protocol_details.json")


def extract_slug_from_logo(logo_url: str) -> str | None:
    """
    https://icons.llamao.fi/icons/protocols/ether.fi -> ether.fi
    """
    if not logo_url:
        return None
    return logo_url.rstrip("/").split("/")[-1]


def build_governance_id(url: str) -> str | None:
    """
    Build governance_id directly from DefiLlama governance-cache URL

    Examples:
    snapshot/etherfi-dao.eth.json
      -> snapshot:etherfi-dao.eth

    compound/ethereum/0xabc.json
      -> compound:ethereum:0xabc

    tally/eip155/1/0xabc.json
      -> tally:eip155:1:0xabc
    """
    if not url:
        return None

    parsed = urlparse(url)
    path = parsed.path

    if "/governance-cache/" not in path:
        return None

    after_cache = path.split("/governance-cache/", 1)[1]
    after_cache = after_cache.removesuffix(".json")

    parts = after_cache.strip("/").split("/")

    if len(parts) < 2:
        return None

    governance_type = parts[0]
    governance_key = ":".join(parts[1:])

    return f"{governance_type}:{governance_key}"


def main():
    # Load files
    with open(PROTOCOL_GOVERNANCE_FILE, "r", encoding="utf-8") as f:
        governance_data = json.load(f)

    with open(PROTOCOL_DETAILS_FILE, "r", encoding="utf-8") as f:
        protocol_details = json.load(f)

    # Build lookup: slug -> governance_id
    governance_by_slug = {}

    for g in governance_data:
        slug = g.get("slug")
        url = g.get("url")

        governance_id = build_governance_id(url)

        if slug and governance_id:
            governance_by_slug[slug] = governance_id

    # Merge into protocol_details
    updated = 0

    for protocol in protocol_details:
        slug = extract_slug_from_logo(protocol.get("logo"))
        if not slug:
            continue

        governance_id = governance_by_slug.get(slug)
        if governance_id:
            protocol["governance_id"] = governance_id
            updated += 1

    # Write back
    with open(PROTOCOL_DETAILS_FILE, "w", encoding="utf-8") as f:
        json.dump(protocol_details, f, indent=2)

    print(f"✅ Updated governance_id for {updated} protocols")


if __name__ == "__main__":
    main()
