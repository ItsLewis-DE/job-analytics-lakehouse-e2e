from bs4 import BeautifulSoup 
import json
from seleniumbase import SB
import time
from pathlib import Path
import boto3
WORKING_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = WORKING_DIR / 'data'

def extract_job_details(soup):
    try:
        deadline_elem = soup.select_one('.deadline >strong')
        deadline = deadline_elem.text if deadline_elem else None
        
        title_elem = soup.select_one('.job-detail__info--title')
        job_title = title_elem.text.strip() if title_elem else None
        
        salary_elem = soup.select_one('.section-salary .job-detail__info--section-content-value')
        salary = salary_elem.text.strip() if salary_elem else None
        
        place_elem = soup.select_one('.section-location .job-detail__info--section-content-value a')
        place = place_elem.text.strip() if place_elem else None
        
        experience_elem = soup.select_one('.section-experience .job-detail__info--section-content-value')
        experience = experience_elem.text.strip() if experience_elem else None
        
        yeu_cau = []
        chuyen_mon = []
        for i, group in enumerate(soup.select('.job-tags__group')):
            if i == 0:
                tags = group.select('.job-tags__group-list-tag-scroll .search-from-tag')
                yeu_cau = [tag.text.strip() for tag in tags]
            else:
                link_tags = group.select('.link')
                if link_tags:
                    chuyen_mon = [link_tag.text.strip() for link_tag in link_tags]
                    
        name_company_elem = soup.select_one('.company-name-label >a')
        name_company = name_company_elem.text.strip() if name_company_elem else None
        
        scale_elem = soup.select_one('.company-scale .company-value')
        scale = scale_elem.text.strip() if scale_elem else None
        
        field_elem = soup.select_one('.company-field .company-value')
        field = field_elem.text.strip() if field_elem else None
        
        address_elem = soup.select_one('.company-address .company-value')
        address = address_elem.text.strip() if address_elem else None
        
        link_company_elem = soup.select_one('.job-detail__company--link >a')
        link_company = link_company_elem['href'] if link_company_elem else None
            
        return {
            'job_title': job_title,
            'salary': salary,
            'deadline': deadline,
            'place': place,
            'experience': experience,
            'yeu_cau': yeu_cau,
            'chuyen_mon': chuyen_mon,
            'name_company': name_company,
            'scale': scale,
            'field': field,
            'address': address,
            'link_company': link_company
        }
    except Exception as e:
        print(f"Lỗi khi parse chi tiết công việc: {e}")
        return None

def crawl_data():
    all_jobs_data = []
    page = 1
    
    with SB(uc=True, headless=True) as sb:
        while True:
            url = f'https://www.topcv.vn/tim-viec-lam-data-kcr257?type_keyword=1&page={page}&category_family=r257&saturday_status=0&sba=1'
            print(f"=== Đang mở trang danh sách việc làm trang {page} ===")
            sb.get(url)
            sb.sleep(3)
            html = sb.get_page_source()
            soup = BeautifulSoup(html, 'lxml')
            
            job_links = soup.select('.body-box .title > a')
            if not job_links:
                print(f"Không tìm thấy việc làm nào trên trang {page}. Đã duyệt hết các trang!")
                break
                
            job_urls = [link['href'] for link in job_links]
            print(f"Tìm thấy {len(job_urls)} công việc trên trang {page}")
            
            for job_url in job_urls:
                print(f"Đang lấy chi tiết: {job_url}")
                try:
                    sb.get(job_url)
                    sb.sleep(3)
                    html_job = sb.get_page_source()
                    job_soup = BeautifulSoup(html_job, 'lxml')
                    
                    job_data = extract_job_details(job_soup)
                    if job_data:
                        job_data['job_url'] = job_url
                        all_jobs_data.append(job_data)
                        print(f"-> Đã lấy thành công: {job_data['job_title']}")
                except Exception as e:
                    print(f"Lỗi khi truy cập {job_url}: {e}")
            
            # Lưu dữ liệu sau mỗi trang để tránh mất mát nếu có lỗi
            DATA_DIR.mkdir(parents=True,exist_ok=True)
            with open(f'{DATA_DIR}/topcv_jobs.json', 'w', encoding='utf-8') as f:
                json.dump(all_jobs_data, f, ensure_ascii=False, indent=4)
                
            page += 1
            
    print(f"Hoàn thành! Đã lấy xong tổng cộng {len(all_jobs_data)} việc làm và lưu vào file topcv_jobs.json")

if __name__ == "__main__":
    crawl_data()
