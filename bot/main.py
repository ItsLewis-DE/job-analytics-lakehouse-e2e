import os
import discord
from discord.ext import commands
from dotenv import load_dotenv
import trino_client

load_dotenv()

# Khởi tạo bot với prefix '!'
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!job ', intents=intents, help_command=None)

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user} (ID: {bot.user.id})')
    print('------')

@bot.command()
async def help(ctx):
    """Hiển thị danh sách các lệnh."""
    embed = discord.Embed(title="Job Analyst Bot - Hướng dẫn sử dụng", color=0x3498db)
    embed.add_field(name="!job search <từ_khóa>", value="Tìm kiếm việc làm theo chức danh.", inline=False)
    embed.add_field(name="!job stats", value="Thống kê số lượng việc làm theo từng nguồn.", inline=False)
    embed.add_field(name="!job skills", value="Xem top 10 kỹ năng đang hot nhất.", inline=False)
    embed.add_field(name="!job locations", value="Xem top địa điểm tuyển dụng nhiều nhất.", inline=False)
    await ctx.send(embed=embed)

class JobPagination(discord.ui.View):
    def __init__(self, jobs, keyword):
        super().__init__(timeout=120)
        self.jobs = jobs
        self.keyword = keyword
        self.current_page = 0
        self.items_per_page = 5
        self.max_page = max(0, (len(jobs) - 1) // self.items_per_page)

    def get_embed(self):
        embed = discord.Embed(
            title=f"🔎 Kết quả: {self.keyword} ({len(self.jobs)} việc làm)", 
            color=0x2ecc71,
            description=f"Trang {self.current_page + 1}/{self.max_page + 1}"
        )
        
        start_idx = self.current_page * self.items_per_page
        end_idx = start_idx + self.items_per_page
        current_jobs = self.jobs[start_idx:end_idx]

        for job in current_jobs:
            title = job['job_title'] or "Không rõ"
            company = job['company_name'] or "Không rõ công ty"
            
            salary = "Thỏa thuận"
            if job['salary_min'] and job['salary_max']:
                salary = f"{job['salary_min']:,.0f} - {job['salary_max']:,.0f} {job['salary_currency']}"
            elif job['salary_min']:
                salary = f"Từ {job['salary_min']:,.0f} {job['salary_currency']}"
            elif job['salary_max']:
                salary = f"Lên đến {job['salary_max']:,.0f} {job['salary_currency']}"

            location = job['location_name'] or "Nhiều nơi"
            url = job['job_url'] or "#"
            level = job.get('level_name') or "Không yêu cầu"
            deadline = job.get('deadline_date') or "Chưa rõ"
            posted = job.get('inserted_at') or "Chưa rõ"
            if posted and len(str(posted)) > 10:
                posted = str(posted)[:10]

            embed.add_field(
                name=f"🏢 {company}", 
                value=f"**[{title}]({url})**\n🎓 Level: {level}\n💰 Lương: {salary}\n📍 Địa điểm: {location}\n📅 Đăng: {posted} | Hạn nộp: {deadline}", 
                inline=False
            )
        return embed

    def update_buttons(self):
        self.children[0].disabled = (self.current_page == 0)
        self.children[1].disabled = (self.current_page == self.max_page)

    @discord.ui.button(label="◀ Trước", style=discord.ButtonStyle.primary, custom_id="prev_btn")
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    @discord.ui.button(label="Sau ▶", style=discord.ButtonStyle.primary, custom_id="next_btn")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page += 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

@bot.command(aliases=['s', 'timkiem'])
async def search(ctx, *, keyword: str):
    """Tìm kiếm việc làm."""
    async with ctx.typing():
        jobs = trino_client.search_jobs(keyword, limit=100)
        
        if not jobs:
            await ctx.send(f"❌ Không tìm thấy việc làm nào với từ khóa `{keyword}`.")
            return

        view = JobPagination(jobs, keyword)
        view.update_buttons()
        await ctx.send(embed=view.get_embed(), view=view)

@bot.command()
async def stats(ctx):
    """Xem số lượng việc làm theo nguồn."""
    async with ctx.typing():
        data = trino_client.get_stats()
        if not data:
            await ctx.send("❌ Không lấy được dữ liệu thống kê.")
            return

        embed = discord.Embed(title="📊 Thống kê Việc làm", color=0xf1c40f)
        total = 0
        for source, count in data:
            embed.add_field(name=str(source).capitalize(), value=f"{count:,} việc làm", inline=True)
            total += count
            
        embed.description = f"**Tổng cộng:** {total:,} tin tuyển dụng đã thu thập."
        await ctx.send(embed=embed)

@bot.command()
async def skills(ctx):
    """Top 10 kỹ năng."""
    async with ctx.typing():
        data = trino_client.get_top_skills(10)
        if not data:
            await ctx.send("❌ Không lấy được dữ liệu kỹ năng.")
            return

        embed = discord.Embed(title="🔥 Top 10 Kỹ năng Hot nhất", color=0xe74c3c)
        text = ""
        for i, (skill, count) in enumerate(data, 1):
            text += f"**{i}.** {skill} ({count:,} tin)\n"
            
        embed.description = text
        await ctx.send(embed=embed)

@bot.command()
async def locations(ctx):
    """Top địa điểm tuyển dụng."""
    async with ctx.typing():
        data = trino_client.get_top_locations(5)
        if not data:
            await ctx.send("❌ Không lấy được dữ liệu địa điểm.")
            return

        embed = discord.Embed(title="📍 Top 5 Địa điểm tuyển dụng", color=0x9b59b6)
        text = ""
        for i, (loc, count) in enumerate(data, 1):
            text += f"**{i}.** {loc}: {count:,} việc làm\n"
            
        embed.description = text
        await ctx.send(embed=embed)

if __name__ == "__main__":
    token = os.getenv("DISCORD_BOT_TOKEN")
    if not token or token == "your_token_here":
        print("Lỗi: Chưa cung cấp DISCORD_BOT_TOKEN trong file .env")
    else:
        bot.run(token)
