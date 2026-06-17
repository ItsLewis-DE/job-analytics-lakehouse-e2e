import os
import sys
from bs4 import BeautifulSoup
from datetime import datetime as dt

# Add current directory to path to import base_crawler
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from base_crawler import BaseCrawler

class TopcvCrawler(BaseCrawler):
    def __init__(self):
        super().__init__(site_name='topcv', start_url='https://www.topcv.vn/tim-viec-lam-data-kcr257?type_keyword=1&category_family=r257&saturday_status=0&sba=1')

    def extract_job_details(self, soup):
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
            
            info_add = soup.select('.box-general-content >.box-general-group')
            level = None
            education = None
            working_type = None
            working_day = None
            for info in info_add:
                name_ele = info.select_one('.box-general-group-info-title')
                name = name_ele.text.strip() if name_ele else None
                value_ele = info.select_one('.box-general-group-info-value')
                value = value_ele.text.strip() if value_ele else None
                if name=='Cấp bậc':
                    level = value
                elif name =='Học vấn':
                    education = value
                elif name=='Hình thức làm việc':
                    working_type=value
                elif name =='Loại hình làm việc':
                    working_day = value

            working_hour_ele = soup.select_one('.job-description__item--content-list')
            working_hour = working_hour_ele.text.strip() if working_hour_ele else None

            skills = []
            for list_job in soup.select('.box-category.collapsed'):
                skill_ele = list_job.select('.box-category-tag')
                for skill in skill_ele:
                    if skill:
                        skills.append(skill.text.strip())
            if not job_title:
                return None
                
            return {
                'job_title': job_title,
                'salary': salary,
                'deadline': deadline,
                'place': place,
                'experience': experience,
                'level': level,
                'education': education,
                'working_type': working_type,
                'working_day': working_day,
                'working_hour': working_hour,
                'skills': skills,
                'name_company': name_company,
                'scale': scale,
                'field': field,
                'address': address,
                'link_company': link_company,
                'inserted_at': dt.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        except Exception as e:
            self.logger.error(f"Lỗi khi parse chi tiết công việc: {e}")
            return None

    def do_crawl(self, sb):
        page = 1
        len_total =0
        while True:
            page_data = []
            url = f'{self.start_url}&page={page}'
            self.logger.info(f"=== Đang mở trang danh sách việc làm trang {page} ===")
            sb.get(url)
            sb.sleep(3)
            html = sb.get_page_source()
            soup = BeautifulSoup(html, 'lxml')
            
            job_links = soup.select('.body-box .title > a')
            if not job_links:
                self.logger.info(f"Không tìm thấy việc làm nào trên trang {page}. Đã duyệt hết các trang!")
                break
                
            job_urls = [link['href'] for link in job_links]
            self.logger.info(f"Tìm thấy {len(job_urls)} công việc trên trang {page}")
            
            for job_url in job_urls:
                self.logger.info(f"Đang lấy chi tiết: {job_url}")
                try:
                    sb.uc_open_with_reconnect(job_url, 4)
                    sb.sleep(3)
                    
                    # Kiểm tra xem có bị dính Cloudflare không
                    if sb.is_text_visible("Just a moment", "title") or sb.is_element_visible("#challenge-error-text"):
                        self.logger.warning(f"Bị Cloudflare chặn ở URL: {job_url}. Đang thử bypass...")
                        sb.uc_gui_click_captcha()
                        sb.sleep(3)
                        
                    html_job = sb.get_page_source()
                    job_soup = BeautifulSoup(html_job, 'lxml')
                    
                    job_data = self.extract_job_details(job_soup)
                    if job_data:
                        job_data['job_url'] = job_url
                        page_data.append(job_data)
                        self.logger.info(f"-> Đã lấy thành công: {job_data['job_title']}")
                except Exception as e:
                    self.logger.error(f"Lỗi khi truy cập {job_url}: {e}")
                    
            if page_data:
                self.save_to_json(page_data)

            len_total += len(page_data)
            page += 1
            
        self.logger.info(f"Hoàn thành! Đã lấy xong tổng cộng {len_total} việc làm.")

if __name__ == "__main__":
    crawler = TopcvCrawler()
    crawler.run()
