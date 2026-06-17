import os
import sys
from bs4 import BeautifulSoup
from datetime import datetime as dt

# Add current directory to path to import base_crawler
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from base_crawler import BaseCrawler

class TopdevCrawler(BaseCrawler):
    def __init__(self):
        super().__init__(site_name='topdev', start_url='https://topdev.vn/jobs/search?job_categories_ids=3%2C2%2C8')

    def extract_job_details(self, soup):
        try:
            title_elem = soup.select_one('.flex.w-full.flex-col.justify-between >a')
            job_title = title_elem.text.strip() if title_elem else None
            
            place = None
            experience=None
            level=None
            for i,info in enumerate(soup.select('.grid.grid-cols-2 .flex.items-center.gap-1')):
                if i==0:
                    place = info.select_one('.line-clamp-1').text.strip()
                elif i==1:
                    level = info.text.strip()
                else:
                    experience = info.text.strip()

            deadline=None
            deadline_ele = soup.select_one('.flex.items-center.gap-2.text-sm.text-text-900')
            if deadline_ele:
                list_deadline = list(deadline_ele.stripped_strings)
                if list_deadline:
                    deadline = list_deadline[0]
            
            demands = []
            benefits = []
            working_time = []
            list_info = soup.select('.border-text-200.text-text-200.rounded-xl.border.p-4 .mt-2') 
            if list_info:
                for i,info in enumerate(list_info):
                    if i==1:
                        demands = [demand.text.strip() for demand in info.select('li')]
                    if i==2:
                        benefits = [benefit.text.strip() for benefit in info.select('.prose-ul.text-text-900 li')]
                        for j,items in enumerate(info.select('ul')):
                            if j==1:
                                for li in items.select('li'):
                                    working_time.append(li.text.strip())
            company_industry = None
            company_country = None
            company_size = None
            company_info = soup.select_one('.relative.flex.flex-col.p-6.pb-2 >a')
            
            company_link = None
            company_name = None
            if company_info:
                company_link = company_info.get('href')
                if company_link:
                    company_link = 'https://topdev.vn' + company_link
                company_name_ele = company_info.select_one('span')
                company_name = company_name_ele.text.strip() if company_name_ele else None
                list_info = company_info.select('.flex.items-center.justify-between.gap-1')
                for i,info in enumerate(list_info):
                    value_ele = info.select_one('.font-semibold')
                    if value_ele:
                        value = value_ele.text.strip() if value_ele else None
                    if i==0:
                        company_industry = value
                    elif i==1:
                        company_size = value
                    else:
                        company_country = value

            return {
                'job_title': job_title,
                'place': place,
                'experience': experience,
                'level': level,
                'deadline': deadline,
                'demands': demands,
                'benefits': benefits,
                'working_time': working_time,
                'company_name':company_name,
                'company_size':company_size,
                'company_industry':company_industry,
                'company_country':company_country,
                'company_link':company_link,
                'inserted_at': dt.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        except Exception as e:
            self.logger.error(f"Lỗi khi parse chi tiết công việc: {e}")
            return None

    def do_crawl(self, sb):
        page=1
        len_total =0
        while True:
            page_data = []
            url = f'{self.start_url}&page={page}'
            self.logger.info(f"=== Đang mở trang danh sách việc làm trang {page} ===")
            sb.get(url)
            sb.sleep(5)
            html = sb.get_page_source()
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract job links
            job_links_tags = soup.find_all('a')
            job_urls = []
            for a in job_links_tags:
                href = a.get('href')
                if href and ('/detail-jobs/' in href or '/viec-lam/' in href):
                    if href.startswith('/'):
                        href = 'https://topdev.vn' + href
                    if href not in job_urls:
                        job_urls.append(href)
            if not job_urls:
                self.logger.info(f"Không tìm thấy việc làm nào trên trang {page}. Đã duyệt hết các trang!")
                break
            
            self.logger.info(f"Tìm thấy {len(job_urls)} công việc trên trang {page}")
            
            for job_url in job_urls:
                self.logger.info(f"Đang lấy chi tiết: {job_url}")
                try:
                    sb.get(job_url)
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
            
            # Lưu dữ liệu sau mỗi trang để tránh mất mát nếu có lỗi
            if page_data:
                self.save_to_json(page_data)

            len_total += len(page_data)
            page+=1
                
        self.logger.info(f"Hoàn thành! Đã lấy xong tổng cộng {len_total} việc làm.")

if __name__ == "__main__":
    crawler = TopdevCrawler()
    crawler.run()