import os
import sys
from bs4 import BeautifulSoup
from datetime import datetime as dt
import re

# Add current directory to path to import base_crawler
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from base_crawler import BaseCrawler

class CarrervietCrawler(BaseCrawler):
    def __init__(self):
        super().__init__(site_name='careerviet', start_url='https://careerviet.vn/viec-lam/data-k-vi.html')

    def extract_job_details(self, soup):
        try:
            job_title_el = soup.select_one('h1.title, h1')
            job_title = job_title_el.text.strip() if job_title_el else None
            
            company_name_el = soup.select_one('a.employer.job-company-name, .job-company-name, .company-name, a.name')
            company_name = company_name_el.text.strip() if company_name_el else None

            salary = None
            experience = None
            level = None
            industry = None
            deadline = None
            place = None
            
            map_el = soup.select_one('.map p a, .map p')
            if map_el:
                place = map_el.text.strip()
            
            requirements = []
            benefits = []
            skills = []
            
            top_info = soup.select('.detail-box li')
            for li in top_info:
                strong = li.find('strong')
                p = li.find('p')
                if strong and p:
                    label = strong.text.strip().lower()
                    val = p.text.strip()
                    if 'lương' in label:
                        salary = val
                    elif 'kinh nghiệm' in label:
                        experience = val
                    elif 'cấp bậc' in label:
                        level = val
                    elif 'ngành nghề' in label:
                        industry = val
                    elif 'hết hạn' in label:
                        deadline = val
                    
            # Detail headings for maximum data
            headings = soup.select('h2, h3, h4, .title-info')
            for h in headings:
                heading_text = h.text.strip().lower()
                nxt = h.find_next_sibling()
                if nxt:
                    items = []
                    lis = nxt.find_all('li')
                    a_tags = nxt.find_all('a')
                    
                    if lis:
                        items = [li.text.strip() for li in lis if li.text.strip()]
                    elif a_tags and ('job tags' in heading_text or 'kỹ năng' in heading_text):
                        items = [a.text.strip() for a in a_tags if a.text.strip()]
                    else:
                        items = [s for s in nxt.stripped_strings if s.strip()]

                    if 'yêu cầu' in heading_text:
                        requirements.extend(items)
                    elif 'phúc lợi' in heading_text:
                        benefits.extend(items)
                    elif 'job tags' in heading_text or 'kỹ năng' in heading_text:
                        skills.extend(items)

            return {
                'job_title': job_title,
                'company_name': company_name,
                'place': place,
                'salary': salary,
                'experience': experience,
                'level': level,
                'industry': industry,
                'deadline': deadline,
                'requirements': requirements,
                'benefits': benefits,
                'skills': skills,
                'inserted_at': dt.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        except Exception as e:
            self.logger.error(f"Lỗi khi parse chi tiết công việc: {e}")
            return None

    def do_crawl(self, sb):
        page = 1
        total_job = 0
        cloudflare_fail_count = 0
        timeout_reached = False
        
        import signal
        class ProcessTimeoutException(BaseException): pass
        def timeout_handler(signum, frame):
            raise ProcessTimeoutException("Quá thời gian 30s")
        signal.signal(signal.SIGALRM, timeout_handler)
        
        while True:
            if cloudflare_fail_count > 5:
                break
                
            page_data = []
            if page == 1:
                url = self.start_url
            else:
                url = self.start_url.replace('-k-vi.html', f'-k-trang-{page}-vi.html')
                
            self.logger.info(f"=== Đang mở trang danh sách việc làm trang {page} ===")
            if hasattr(sb, 'uc_open_with_reconnect'): #Kiểm tra xem có hàm đó kh
                sb.uc_open_with_reconnect(url, 4) # hàm này dùng để đợi 4 giây kiểm tra 
            else:
                sb.get(url)
            sb.sleep(5)
            
            page_title = sb.get_title()
            if "Just a moment" in page_title or "Cloudflare" in page_title or sb.is_element_visible("#challenge-error-text"):
                self.logger.warning(f"Bị Cloudflare chặn ở trang danh sách. Đang thử bypass...")
                if hasattr(sb, 'uc_gui_click_captcha'):
                    sb.uc_gui_click_captcha()
                sb.sleep(4)
                if "Just a moment" in sb.get_title() or "Cloudflare" in sb.get_title() or sb.is_element_visible("#challenge-error-text"):
                    raise Exception("Không thể truy cập trang này dừng luôn hệ thống nha huhu")
            
            html = sb.get_page_source()
            soup = BeautifulSoup(html, 'lxml')
            
            job_links_tags = soup.find_all('a')
            job_urls = []
            for a in job_links_tags:
                href = a.get('href')
                if href and ('/tim-viec-lam/' in href or '/viec-lam/' in href) and '/nha-tuyen-dung/' not in href and re.search(r'\.[A-Z0-9]{8}\.html$', href):
                    if href.startswith('/'):
                        href = 'https://careerviet.vn' + href
                    if 'careerviet.vn' in href and href not in job_urls:
                        job_urls.append(href)
            
            if not job_urls:
                self.logger.info(f"Không tìm thấy việc làm nào trên trang {page}. Đã duyệt hết các trang!")
                break
                
            self.logger.info(f"Tìm thấy {len(job_urls)} công việc trên trang {page}")
            
            cloudflare_fail_count = 0
            for job_url in job_urls:
                if cloudflare_fail_count > 5:
                    break
                
                if total_job + len(page_data) >= 200:
                    self.logger.info("Đã đạt giới hạn 200 việc làm. Dừng crawl ở trang này.")
                    break
                    
                self.logger.info(f"Đang lấy chi tiết: {job_url}")
                try:
                    signal.alarm(30) #30 giây đếm ngược nhe
                    sb.get(job_url)
                    
                    page_title = sb.get_title()
                    if "Just a moment" in page_title or "Cloudflare" in page_title or sb.is_element_visible("#challenge-error-text"):
                        self.logger.warning(f"Bị Cloudflare chặn ở URL: {job_url}. Đang thử bypass...")
                        if hasattr(sb, 'uc_gui_click_captcha'):
                            sb.uc_gui_click_captcha()
                        sb.sleep(4)
                        if "Just a moment" in sb.get_title() or "Cloudflare" in sb.get_title() or sb.is_element_visible("#challenge-error-text"):
                            self.logger.warning("Vẫn bị chặn, click thử lại...")
                            if hasattr(sb, 'uc_gui_click_captcha'):
                                sb.uc_gui_click_captcha()
                            sb.sleep(4)
                            if "Just a moment" in sb.get_title() or "Cloudflare" in sb.get_title() or sb.is_element_visible("#challenge-error-text"):
                                cloudflare_fail_count += 1
                                signal.alarm(0) # Tắt đếm ngược trước khi skip
                                continue
                    
                    import random
                    sb.sleep(random.uniform(3, 6))
                    
                    html_job = sb.get_page_source()
                    job_soup = BeautifulSoup(html_job, 'lxml')
                    
                    job_data = self.extract_job_details(job_soup)
                    if job_data:
                        job_data['job_url'] = job_url
                        page_data.append(job_data)
                        self.logger.info(f"-> Đã lấy thành công: {job_data['job_title']}")
                        cloudflare_fail_count = 0
                    signal.alarm(0)
                except ProcessTimeoutException as e: #class này đã được tạo bên trên
                    self.logger.warning(f"Dừng task sớm do xử lý URL quá 30s: {job_url}")
                    timeout_reached = True
                    break
                except Exception as e:
                    signal.alarm(0)
                    self.logger.error(f"Lỗi khi truy cập {job_url}: {e}")
                    if "Connection refused" in str(e) or "Max retries exceeded" in str(e) or "not connected to DevTools" in str(e):
                        self.logger.error("Trình duyệt đã crash hoặc mất kết nối WebDriver. Dừng task để Airflow retry!")
                        break
                
            if page_data:
                self.save_to_json(page_data)

            total_job += len(page_data)
            
            if total_job >= 150 or timeout_reached:
                self.logger.info("Đã đạt giới hạn 150 việc làm hoặc timeout. Kết thúc toàn bộ quá trình crawl.")
                break
                
            page += 1
            
        self.logger.info(f"Hoàn thành! Đã lấy xong tổng cộng {total_job} việc làm.")

if __name__ == "__main__":
    crawler = CarrervietCrawler()
    crawler.run()