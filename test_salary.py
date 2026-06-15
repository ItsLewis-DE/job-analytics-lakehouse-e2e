import requests
from bs4 import BeautifulSoup
from seleniumbase import SB

urls = [
    "https://itviec.com/it-jobs/engineering-lead-technical-lead-ai-llm-skylink-labs-4057",
    "https://itviec.com/it-jobs/data-intern-3-months-itviec-0700",
    "https://itviec.com/it-jobs/python-developer-intern-sql-aws-bosch-global-software-technologies-company-limited-5851" # from screenshot
]
with SB(uc=True, headless=True) as sb:
    for url in urls:
        print("URL:", url)
        sb.get(url)
        sb.sleep(2)
        html = sb.get_page_source()
        soup = BeautifulSoup(html, 'lxml')
        sal = soup.select_one('.salary')
        if sal:
            print("Found .salary text:", repr(sal.text.strip()))
        else:
            print("NOT FOUND .salary")
