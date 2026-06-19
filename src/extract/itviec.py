import os
import sys
from bs4 import BeautifulSoup
from datetime import datetime as dt

from base_crawler import BaseCrawler

class ItviecCrawler(BaseCrawler):
    def __init__(self):
        super().__init__(site_name='itviec', start_url='https://itviec.com/segments/viec-lam-ai-data?gclid=Cj0KCQjw_7PRBhDcARIsAMjV7jnMzspHCFzRYRiAt0Q6MehzxrAVMZCXYyY3pCaXoCbQ7inNq8KDKCcaAtX3EALw_wcB&utm_campaign=gsn_brand_hcm_mc&utm_content=itviec_ad2&utm_medium=cpc&utm_source=google&utm_term=itviec&click_action=banner_clicked&touchpoint_type=header_menu')

    def extract_job_details(self, soup):
        try:
            title_elem = soup.select_one('h1')
            job_title = title_elem.text.strip() if title_elem else None
            
            place_elem = soup.select_one('.normal-text.text-rich-grey')
            place = place_elem.text.strip() if place_elem else None
            
            is_accepted_intern_ele = soup.select_one('.text-reset.normal-text')
            is_accepted_intern = is_accepted_intern_ele.text.strip() if is_accepted_intern_ele else None

            working_type_ele = soup.select_one('.normal-text.text-rich-grey.ms-1')
            working_type = working_type_ele.text.strip() if working_type_ele else None

            skills = []
            skills_list = soup.select('.d-flex.flex-wrap.igap-2 >a')
            for skill_ele in skills_list:
                skill = skill_ele.text.strip() if skill_ele else None
                if skill:
                    skills.append(skill)
            
            company_name = None
            company_industry=None
            company_size=None
            company_place=None
            working_days=None

            list_info_company = soup.select('.imt-4 > .row.ipy-2.gx-0.border-bottom-dashed')
            for info in list_info_company:
                type_ele = info.select_one('.col.text-dark-grey')
                type = type_ele.text.strip() if type_ele else None
                if type == 'Company type':
                    company_name = info.select_one('.col.text-end.text-it-black').text.strip()
                elif type == 'Company industry':
                    company_industry = info.select_one('.d-inline-flex.text-wrap').text.strip()
                elif type == 'Company size':
                    company_size = info.select_one('.col.text-end.text-it-black').text.strip()
                elif type=='Country':
                    company_place = info.select_one('span.align-middle').text.strip()
                elif type=='Working days':
                    working_days = info.select_one('.col.text-end.text-it-black').text.strip()

            company_link_ele = soup.select_one('.ipt-3.ipt-xl-1.ipb-2.text-clamp-3 >a ')
            company_link = company_link_ele.get('href') if company_link_ele else None
            if company_link:
                company_link = 'https://itviec.com' + company_link

            list_demand = soup.select('.imy-5.paragraph')
            job_demands = []
            job_benefits = []
            for i, item in enumerate(list_demand):
                if i == 1:
                    demand_ele = item.select('li')
                    for demand in demand_ele:
                        job_demands.append(demand.text.strip())
                elif i == len(list_demand)-1:
                    benefit_ele = item.select('li')
                    for benefit in benefit_ele:
                        job_benefits.append(benefit.text.strip())

            return {
                'job_title': job_title,
                'place': place,
                'is_accepted_intern': is_accepted_intern,
                'working_type': working_type,
                'skills': skills,
                'company_name': company_name,
                'company_industry': company_industry,
                'company_size': company_size,
                'company_place': company_place,
                'company_link': company_link,
                'working_days': working_days,
                'demands': job_demands,
                'benefits': job_benefits,
                'inserted_at': dt.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        except Exception as e:
            self.logger.error(f"Lỗi khi parse chi tiết công việc: {e}")
            return None

    def do_crawl(self, sb):
        cloudflare_fail_count = 0
        self.logger.info(f"=== Đang mở trang danh sách việc làm ===")
        if hasattr(sb, 'uc_open_with_reconnect'):
            sb.uc_open_with_reconnect(self.start_url, 4)
        else:
            sb.get(self.start_url)
        sb.sleep(3)
        
        page_title = sb.get_title()
        if "Just a moment" in page_title or "Cloudflare" in page_title or sb.is_element_visible("#challenge-error-text"):
            self.logger.warning(f"Bị Cloudflare chặn ở trang danh sách. Đang thử bypass...")
            if hasattr(sb, 'uc_gui_click_captcha'):
                sb.uc_gui_click_captcha()
            sb.sleep(4)
            if "Just a moment" in sb.get_title() or "Cloudflare" in sb.get_title() or sb.is_element_visible("#challenge-error-text"):
                cloudflare_fail_count += 1
                if cloudflare_fail_count > 4:
                    self.logger.error("Bị Cloudflare chặn cứng ở trang danh sách. Dừng Crawler!")
                    import sys
                    sys.exit(1)
                else:
                    self.logger.warning(f"Bị chặn lần {cloudflare_fail_count}/4. Sẽ tiếp tục bỏ qua danh sách này nhưng vì không có URL nào nên coi như không có data.")
                    return
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
                   
        self.logger.info(f"Tìm thấy {len(job_urls)} công việc trên trang")
        
        job_total = 0
        all_jobs_data = []
        for job_url in job_urls:
            if cloudflare_fail_count > 10:
                import sys
                sys.exit(1)
            if job_total >= 200:
                self.logger.info("Đã đạt giới hạn 200 việc làm. Dừng crawl.")
                break
                
            job_total += 1
            self.logger.info(f"Đang lấy chi tiết: {job_url}")
            try:
                if hasattr(sb, 'uc_open_with_reconnect'):
                    sb.uc_open_with_reconnect(job_url, 4)
                else:
                    sb.get(job_url)
                sb.sleep(3)
                
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
                            continue
                            
                html_job = sb.get_page_source()
                job_soup = BeautifulSoup(html_job, 'lxml')
                
                job_data = self.extract_job_details(job_soup)
                if job_data:
                    job_data['job_url'] = job_url
                    all_jobs_data.append(job_data)
                    self.logger.info(f"-> Đã lấy thành công: {job_data['job_title']}")
            
            except Exception as e:
                self.logger.error(f"Lỗi khi truy cập {job_url}: {e}")
                if "Connection refused" in str(e) or "Max retries exceeded" in str(e) or "not connected to DevTools" in str(e):
                    self.logger.error("Trình duyệt đã crash hoặc mất kết nối WebDriver. Dừng task để Airflow retry!")
                    import sys
                    sys.exit(1)
                

            if job_total % 50 == 0:
                self.save_to_json(all_jobs_data)
                all_jobs_data = []
            cloudflare_fail_count=0

        # Lưu dữ liệu cuối cùng
        if all_jobs_data: 
            self.save_to_json(all_jobs_data)
                
        self.logger.info(f"Hoàn thành! Đã lấy xong tổng cộng {job_total} việc làm.")

if __name__ == "__main__":
    crawler = ItviecCrawler()
    crawler.run()
