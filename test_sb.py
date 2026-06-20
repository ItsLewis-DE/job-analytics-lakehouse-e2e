from seleniumbase import SB

with SB(uc=True, headless=True) as sb:
    sb.get("https://careerviet.vn/viec-lam/data-tai-ho-chi-minh-kl8-vi.html")
    sb.sleep(5)
    html = sb.get_page_source()
    with open("careerviet_sb.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("Done fetching with SB!")
