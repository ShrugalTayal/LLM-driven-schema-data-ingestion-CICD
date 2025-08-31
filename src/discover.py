# src/discover.py
import yaml, argparse
from ddgs import DDGS

def discover(cfg, max_results=5):
    queries = cfg.get("search_keywords", [])
    allow = cfg.get("allow_domains", [])
    results = set()
    with DDGS() as ddgs:
        for q in queries:
            for r in ddgs.text(q, max_results=max_results):
                url = r.get("href") or r.get("url")
                if not url: continue
                if allow:
                    if any(d in url for d in allow):
                        results.add(url)
                else:
                    results.add(url)
    return list(results)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    p.add_argument("--output", default="urls.txt")
    args = p.parse_args()
    cfg = yaml.safe_load(open(args.config))
    urls = discover(cfg, max_results=cfg.get("max_results_per_query", 5))
    open(args.output, "w").write("\n".join(urls))
    print(f"Saved {len(urls)} URLs -> {args.output}")