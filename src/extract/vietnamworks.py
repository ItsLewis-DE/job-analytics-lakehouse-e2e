import os
import sys
from bs4 import BeautifulSoup
from datetime import datetime as dt

# Add current directory to path to import base_crawler
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from base_crawler import BaseCrawler

class VietnamworksCrawler(BaseCrawler):
    def __init__(self):
        super().__init__(site_name='vietnamworks', start_url='https://www.vietnamworks.com/viec-lam?q=data&sorting=relevant')

    def extract_job_details(self, soup):
        try:
            title_elem = soup.select_one('.sc-ab270149-0.hAejeW')
            job_title = title_elem.text.strip() if title_elem else None
            
            salary_elem = soup.select_one('.sc-ab270149-0.cVbwLK')
            salary = salary_elem.text.strip() if salary_elem else None

            is_urgent_elem = soup.select_one('.sc-ab270149-0.guKwvE')
            is_urgent = is_urgent_elem.text.strip() if is_urgent_elem else None
            
            deadline_elem = soup.select_one('.sc-ab270149-0.ePOHWr')
            deadline = deadline_elem.text.strip() if deadline_elem else None
            
            place_elem = soup.select_one('.sc-4ab41082-1.gVpPKv .sc-ab270149-0.ePOHWr')
            place = place_elem.text.strip() if place_elem else None

            level=None
            skills=None
            field=None
            experience=None

            # Lấy tất cả các thẻ chứa giá trị thông tin
            value_elements = soup.select('.sc-ab270149-0.cLLblL')
            
            for val_ele in value_elements:
                value = val_ele.text.strip()
                parent = val_ele.parent
                if not parent:
                    continue
                    
                # parent.text chứa cả label và value (vd: "TRÌNH ĐỘ HỌC VẤN TỐI THIỂUCử nhân")
                full_text = parent.text.strip().lower()

                if 'cấp bậc' in full_text:
                    level = value
                elif 'kỹ năng' in full_text:
                    skills = value
                elif 'lĩnh vực' in full_text:
                    field = value
                elif 'kinh nghiệm' in full_text:
                    experience = value

            link_company_elem = soup.select_one('.sc-ab270149-0.egZKeY.sc-f0821106-0.gWSkfE')
            link_company = link_company_elem.get('href') if link_company_elem else None
            name_company = link_company_elem.text.strip() if link_company_elem else None
            
            info_company = soup.select('.sc-37577279-4.kNdlhJ > .sc-37577279-5.kQCIWi')
            address_company =None
            company_size=None
            for i,info in enumerate(info_company):
                value_ele = info.select_one('.sc-ab270149-0.ePOHWr')
                value = value_ele.text.strip() if value_ele else None
                if i==0:
                    address_company = value
                elif i ==1:
                    company_size=value
                    
            return {
                'job_title': job_title,
                'salary': salary,
                'is_urgent': is_urgent,
                'deadline': deadline,
                'place': place,
                'level': level,
                'experience': experience,
                'skills': skills,
                'name_company': name_company,
                'scale': company_size,
                'field': field,
                'address': address_company,
                'link_company': link_company,
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
        seen_urls = set()
        
        import signal
        class ProcessTimeoutException(BaseException): pass
        def timeout_handler(signum, frame):
            raise ProcessTimeoutException("Quá thời gian 30s")
        signal.signal(signal.SIGALRM, timeout_handler)
        while True:
            page_data = []            
            # Construct page URL
            url = f"{self.start_url}&page={page}"
            self.logger.info(f"=== Đang mở trang danh sách việc làm trang {page} ===")
            if hasattr(sb, 'uc_open_with_reconnect'):
                sb.uc_open_with_reconnect(url, 4)
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
            
            # Extract job links
            job_links_tags = soup.find_all('a')
            job_urls = []
            for a in job_links_tags:
                href = a.get('href')
                # Lọc các link việc làm dựa trên /job/ hoặc /viec-lam/ hoặc kết thúc với -jv
                if href and ('-jv' in href or '/viec-lam/' in href or '/job/' in href):
                    if len(href) > 20: 
                        if href.startswith('/'):
                            href = 'https://www.vietnamworks.com' + href
                        if 'vietnamworks.com' in href and href not in seen_urls:
                            seen_urls.add(href)
                            job_urls.append(href)
            
            if not job_urls:
                self.logger.info(f"Không tìm thấy việc làm nào trên trang {page}. Đã duyệt hết các trang!")
                break
                
            self.logger.info(f"Tìm thấy {len(job_urls)} công việc trên trang {page}")
            consecutive_errors = 0
            
            #Truy cập vào bài đăng tuyển dụng đó để lấy thêm thông tin chi tiết
            cloudflare_fail_count =0
            for job_url in job_urls:
                if cloudflare_fail_count > 10:
                    break
                
                if total_job + len(page_data) >= 200:
                    self.logger.info("Đã đạt giới hạn 200 việc làm. Dừng crawl ở trang này.")
                    break
                    
                self.logger.info(f"Đang lấy chi tiết: {job_url}")
                try:
                    signal.alarm(30)
                    sb.get(job_url)
                    import random
                    sb.sleep(random.uniform(3, 6))
                    
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
                    
                    # Click nút "Xem thêm" nếu có để mở rộng toàn bộ thông tin
                    try:
                        sb.click('//*[contains(text(), "Xem thêm")]', by="xpath", timeout=2)
                        sb.sleep(1)
                    except Exception:
                        pass
                        
                    html_job = sb.get_page_source()
                    job_soup = BeautifulSoup(html_job, 'lxml')
                    
                    job_data = self.extract_job_details(job_soup)
                    if job_data:
                        job_data['job_url'] = job_url
                        page_data.append(job_data)
                        self.logger.info(f"-> Đã lấy thành công: {job_data['job_title']}")
                        cloudflare_fail_count = 0
                    signal.alarm(0)
                except ProcessTimeoutException as e:
                    self.logger.warning(f"Dừng task sớm do xử lý URL quá 30s: {job_url}")
                    timeout_reached = True
                    break
                except Exception as e:
                    signal.alarm(0)
                    self.logger.error(f"Lỗi khi truy cập {job_url}: {e}")
                    consecutive_errors += 1
                    if consecutive_errors >= 3 or "Connection refused" in str(e) or "Max retries exceeded" in str(e) or "not connected to DevTools" in str(e) or "renderer" in str(e):
                        self.logger.error("Trình duyệt đã crash hoặc mất kết nối WebDriver. Dừng task để Airflow retry!")
                        import sys
                        sys.exit(1)
                
                
            if page_data:
                self.save_to_json(page_data)

            total_job += len(page_data)
            
            if total_job >= 200 or timeout_reached:
                self.logger.info("Đã đạt giới hạn 200 việc làm hoặc timeout. Kết thúc toàn bộ quá trình crawl.")
                break
                
            page += 1
            
        self.logger.info(f"Hoàn thành! Đã lấy xong tổng cộng {total_job} việc làm.")

if __name__ == "__main__":
    crawler = VietnamworksCrawler()
    crawler.run()