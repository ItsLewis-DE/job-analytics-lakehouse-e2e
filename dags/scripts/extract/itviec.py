import logging
from bs4 import BeautifulSoup
from seleniumbase import SB
import time
import json
import os
from pathlib import Path

WORKING_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = WORKING_DIR / 'data'

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

url_base = 'https://itviec.com/segments/viec-lam-ai-data?gclid=Cj0KCQjw_7PRBhDcARIsAMjV7jnMzspHCFzRYRiAt0Q6MehzxrAVMZCXYyY3pCaXoCbQ7inNq8KDKCcaAtX3EALw_wcB&utm_campaign=gsn_brand_hcm_mc&utm_content=itviec_ad2&utm_medium=cpc&utm_source=google&utm_term=itviec&click_action=banner_clicked&touchpoint_type=header_menu'

def extract_job_details(soup):
    try:
        title_elem = soup.select_one('h1')
        job_title = title_elem.text.strip() if title_elem else None
        
        salary_elem = soup.select_one('.svg-icon__salary')
        if not salary_elem:
            salary_elem = soup.select_one('.job-details__salary')
        if not salary_elem:
            salary_elem = soup.select_one('.salary')
        salary = salary_elem.text.strip() if salary_elem else None
        
        place_elem = soup.select_one('.svg-icon__location')
        if not place_elem:
             place_elem = soup.select_one('.location')
        if not place_elem:
             place_elem = soup.select_one('.job-details__location')
        place = place_elem.text.strip() if place_elem else None
        
        experience_elem = soup.select_one('.svg-icon__experience')
        if not experience_elem:
            experience_elem = soup.select_one('.experience')
        experience = experience_elem.text.strip() if experience_elem else None
        
        yeu_cau = []
        chuyen_mon = []
        # ITviec usually has skills in a tags or span tags
        skill_tags = soup.select('.job-details__tag-list a, .job-details__skill-list span, .tag-list a, .skill-list span, a.anchor.ipp')
        if skill_tags:
            chuyen_mon = [tag.text.strip() for tag in skill_tags]
            
        name_company_elem = soup.select_one('.employer-name, .company-name, .job-details__company-name')
        name_company = name_company_elem.text.strip() if name_company_elem else None
        
        scale_elem = soup.select_one('.svg-icon__group')
        scale = scale_elem.text.strip() if scale_elem else None
        
        field_elem = soup.select_one('.svg-icon__industry')
        field = field_elem.text.strip() if field_elem else None
        
        address_elem = soup.select_one('.svg-icon__location_pin')
        address = address_elem.text.strip() if address_elem else None
        
        return {
            'job_title': job_title,
            'salary': salary,
            'deadline': None, # ITviec might not show deadline explicitly
            'place': place,
            'experience': experience,
            'yeu_cau': yeu_cau,
            'chuyen_mon': chuyen_mon,
            'name_company': name_company,
            'scale': scale,
            'field': field,
            'address': address,
            'link_company': None 
        }
    except Exception as e:
        logger.error(f"Lỗi khi parse chi tiết công việc: {e}")
        return None

def crawl_data():
    all_jobs_data = []
    page = 1
    
    with SB(uc=True, headless=True) as sb:
        while True:
            # Construct page URL
            url = f"{url_base}&page={page}"
            logger.info(f"=== Đang mở trang danh sách việc làm trang {page} ===")
            sb.get(url)
            sb.sleep(3)
            html = sb.get_page_source()
            soup = BeautifulSoup(html, 'lxml')
            
            job_links_tags = soup.select('h3 a')
            job_urls = []
            for a in job_links_tags:
                href = a.get('href')
                if href and '/it-jobs/' in href:
                    if href.startswith('/'):
                        href = 'https://itviec.com' + href
                    job_urls.append(href)
            
            if not job_urls:
                logger.info(f"Không tìm thấy việc làm nào trên trang {page}. Đã duyệt hết các trang!")
                break
                
            logger.info(f"Tìm thấy {len(job_urls)} công việc trên trang {page}")
            
            for job_url in job_urls:
                logger.info(f"Đang lấy chi tiết: {job_url}")
                try:
                    sb.get(job_url)
                    sb.sleep(3)
                    html_job = sb.get_page_source()
                    job_soup = BeautifulSoup(html_job, 'lxml')
                    
                    job_data = extract_job_details(job_soup)
                    if job_data:
                        job_data['job_url'] = job_url
                        all_jobs_data.append(job_data)
                        logger.info(f"-> Đã lấy thành công: {job_data['job_title']}")
                except Exception as e:
                    logger.error(f"Lỗi khi truy cập {job_url}: {e}")
            
            # Lưu dữ liệu sau mỗi trang để tránh mất mát nếu có lỗi
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            with open(f'{DATA_DIR}/itviec_jobs.json', 'w', encoding='utf-8') as f:
                json.dump(all_jobs_data, f, ensure_ascii=False, indent=4)
                
            page += 1
            
    logger.info(f"Hoàn thành! Đã lấy xong tổng cộng {len(all_jobs_data)} việc làm và lưu vào file itviec_jobs.json")

if __name__ == "__main__":
    crawl_data()
