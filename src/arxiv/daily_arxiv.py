#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
@File    :   daily_arxiv.py
@Time    :   2021-10-29 22:34:09
@Author  :   wanghaisheng
@Email   :   edwin_uestc@163.com
@License :   Apache License 2.0
"""

import json
import os
import shutil
import re
import aiohttp
import asyncio
from datetime import datetime
import arxiv
import yaml
from random import randint
import unicodedata
from config import (
    SERVER_PATH_TOPIC,
    SERVER_DIR_STORAGE,
    SERVER_PATH_README,
    SERVER_PATH_DOCS,
    SERVER_PATH_STORAGE_MD,
    SERVER_PATH_STORAGE_BACKUP,
    TIME_ZONE_CN,
    logger
)

# --- Utility Functions ---
class ToolBox:
    @staticmethod
    def log_date(mode="log"):
        now = datetime.now(TIME_ZONE_CN)
        if mode == "log":
            return str(now).split(".")[0]
        elif mode == "file":
            return str(now).split(" ")[0]

    @staticmethod
    def get_yaml_data() -> dict:
        with open(SERVER_PATH_TOPIC, "r", encoding="utf8") as f:
            data = yaml.safe_load(f)
        print("YAML Data:", data)
        return data

    @staticmethod
    async def handle_html(session, url: str):
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/95.0.4638.69 Safari/537.36 Edg/95.0.1020.44"
        }
        try:
            async with session.get(url, headers=headers) as response:
              response.raise_for_status()
              return await response.json()
        except (json.JSONDecodeError, aiohttp.ClientError) as e:
            logger.error(f"Error fetching or decoding JSON from {url}: {e}")
            return None


    @staticmethod
    async def handle_md(session, url: str):
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/95.0.4638.69 Safari/537.36 Edg/95.0.1020.44"
        }
        try:
          async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            return await response.text()
        except aiohttp.ClientError as e:
          logger.error(f"Error fetching MD content from {url}: {e}")
          return None

# --- ArXiv Data Fetching and Parsing ---
class ArxivPaperProcessor:
    def __init__(self, max_results=20):
        self.max_results = max_results

    async def fetch_arxiv_papers(self, keyword):
        try:
            res = arxiv.Search(
                query=f"ti:{keyword}+OR+abs:{keyword}",
                max_results=self.max_results,
                sort_by=arxiv.SortCriterion.SubmittedDate
            ).results()
            return list(res)
        except Exception as e:
            logger.error(f"Error during arXiv query for '{keyword}': {e}")
            return []

    @staticmethod
    def clean_paper_title(title):
        normalized_title = unicodedata.normalize('NFKD', title)
        cleaned_title = re.sub(r'[^\w\s]', '', normalized_title)
        cleaned_title = re.sub(r'\s+', ' ', cleaned_title)
        return cleaned_title.strip()

    async def process_paper_data(self, arxiv_results):
        papers = {}
        base_url = "https://arxiv.paperswithcode.com/api/v0/papers/"
        async with aiohttp.ClientSession() as session:
          for result in arxiv_results:
            paper_id = result.get_short_id()
            paper_title = self.clean_paper_title(result.title)
            paper_url = result.entry_id
            paper_abstract = result.summary.strip().replace('\n', ' ').replace('\r', " ")
            code_url = base_url + paper_id
            paper_first_author = result.authors[0]
            publish_time = result.published.date()
            ver_pos = paper_id.find('v')
            paper_key = paper_id if ver_pos == -1 else paper_id[0:ver_pos]
            logger.info(f"Processing paper ID {paper_id}...")

            repo_url = None
            response = await ToolBox.handle_html(session, code_url)
            if response and response.get("official"):
              repo_url = response["official"].get("url", "null")
            else:
               repo_url = "null"

            papers[paper_key] = {
                "publish_time": publish_time,
                "title": paper_title,
                "authors": f"{paper_first_author} et.al.",
                "id": paper_id,
                "paper_url": paper_url,
                "repo": repo_url,
                "abstract": paper_abstract
            }
        return papers

# --- Markdown Generation ---
def build_frontmatter_appleblog(
    author, cover_image_url, description, keywords, pubdate, tags, title
):
    frontmatter = {
        "author": author,
        "cover": {
            "alt": "cover",
            "square": cover_image_url,
            "url": cover_image_url,
        },
        "description": description,
        "featured": True,
        "keywords": keywords,
        "layout": "../../layouts/MarkdownPost.astro",
        "meta": [
            {"content": author, "name": "author"},
            {"content": keywords, "name": "keywords"},
        ],
        "pubDate": pubdate,
        "tags": tags,
        "theme": "light",
        "title": title,
    }
    return yaml.dump(frontmatter, default_flow_style=False)

class MarkdownGenerator:
    def __init__(self):
        self.update_time = ToolBox.log_date()

    def to_markdown(self, context):
        author = "arxiv-bot"
        cover_image_url = ""  # You would need to generate or fetch a relevant image URL
        description = context["paper"].get("abstract", "No abstract available.")[:200] + '...' # Limit to the first 200 characters
        keywords = [context["topic"],context["subtopic"]] # You might extract actual keywords based on the content
        pubdate = context["paper"].get("publish_time","").isoformat() if context["paper"].get("publish_time") else ToolBox.log_date() # Use publish_time or current time
        tags = [context["topic"], context["subtopic"]]  # You might extract tags based on the content
        title = context["paper"].get("title", "Unknown Paper")
        
        frontmatter = build_frontmatter_appleblog(
            author=author,
            cover_image_url=cover_image_url,
            description=description,
            keywords=", ".join(keywords),
            pubdate=pubdate,
            tags=tags,
            title=title,
        )
        md_content = f"""
        ---\n
        {frontmatter}
        ---\n
        # {title}

        **Publish Time:** {context['paper'].get('publish_time', 'Not available')}
        **Authors:** {context['paper'].get('authors', 'Not available')}
        **PDF Link:** [{context['paper'].get('id', 'Not available')}]({context['paper'].get('paper_url', 'Not available')})
        **Code Repo:** {context['paper'].get('repo', 'Not available')}
        **Abstract:** {context['paper'].get('abstract', 'Not available')}
        """
        return {
            "hook": context["topic"],
            "content": md_content,
        }


    def generate_markdown_template(self, content):
        return f"# Daily ArXiv Updates\n\n{content}"

    def storage(self, content):
        storage_path_by_date = os.path.join(SERVER_DIR_STORAGE, self.update_time)
        if not os.path.exists(storage_path_by_date):
            os.makedirs(storage_path_by_date)

        # Save markdown content
        with open(os.path.join(storage_path_by_date, f"updates_{self.update_time}.md"), "w", encoding="utf8") as f:
            f.write(content)

        # Save readme if it doesn't exist
        if not os.path.exists(SERVER_PATH_README):
            with open(SERVER_PATH_README, "w", encoding="utf8") as f:
                f.write(f"# Daily Updates\n\nUpdates saved in {storage_path_by_date}\n")

        # Copy latest updates to docs directory
        shutil.copytree(storage_path_by_date, SERVER_PATH_DOCS, dirs_exist_ok=True, )


# --- Async Task Management ---
class CoroutineSpeedup:
    def __init__(self, worker_q: asyncio.Queue = None, task_docker=None, power=32):
        self.worker = worker_q if worker_q else asyncio.Queue()
        self.channel = asyncio.Queue()
        self.task_docker = task_docker
        self.power = power
        self.max_queue_size = 0
    

    async def _adaptor(self):
        while True:
            if self.worker.empty():
                if self.worker.empty() and self.channel.empty():
                    break  # Break if both are empty
                await asyncio.sleep(1)  # Avoid busy waiting when the queue is empty
                continue
            task: dict = await self.worker.get()
            if task.get("pending"):
                await self.runtime(context=task.get("pending"))
            elif task.get("response"):
                await self.parse(context=task)


    def offload_tasks(self):
        if self.task_docker:
            for task in self.task_docker:
                self.worker.put_nowait({"pending": task})
        self.max_queue_size = self.worker.qsize()


    async def runtime(self, context: dict):
          keyword = context.get("keyword")
          logger.info(f"Searching for keyword: {keyword}")
          arxiv_processor = ArxivPaperProcessor()
          papers = await arxiv_processor.fetch_arxiv_papers(keyword)
          if papers:
              context["response"] = papers
              context["hook"] = {"topic":context["topic"],"subtopic":context["subtopic"]} # Use a dictionary for topic and subtopic
              await self.worker.put(context)

    async def parse(self, context):
         arxiv_processor = ArxivPaperProcessor()
         papers = await arxiv_processor.process_paper_data(context["response"])
         for paper_key, paper in papers.items():
             await self.channel.put({
                    "paper": paper,
                    "topic": context["hook"]["topic"],
                    "subtopic": context["hook"]["subtopic"],
                    "fields": ["Publish Date", "Title", "Authors", "PDF", "Code", "Abstract"]
             })
         logger.success(f"Handled | topic=`{context['hook']['topic']}` subtopic=`{context['hook']['subtopic']}`")

    async def overload_tasks(self):
        md_generator = MarkdownGenerator()
        file_obj: dict = {}
        while not self.channel.empty():
            context: dict = await self.channel.get()
            md_obj: dict = md_generator.to_markdown(context)

            if not file_obj.get(md_obj["hook"]):
                file_obj[md_obj["hook"]] = ""  # Initialize as an empty string
            file_obj[md_obj["hook"]] += md_obj["content"]

            os.makedirs(os.path.join(SERVER_PATH_DOCS, f'{context["topic"]}'), exist_ok=True)
            with open(os.path.join(SERVER_PATH_DOCS, f'{context["topic"]}', f'{md_obj["hook"]}.md'), "w",
                      encoding="utf8") as f:
                f.write(md_obj["content"])


        if file_obj:
            for key, val in file_obj.items():
                path = os.path.join(SERVER_PATH_DOCS, f"{key}.md")
                with open(path, "w", encoding="utf8") as f:
                   f.write(val)

        md_generator.storage(
           content=md_generator.generate_markdown_template(file_obj),
        )

    async def go(self):
        self.offload_tasks()
        await asyncio.gather(*(self._adaptor() for _ in range(self.power)))
        await self.overload_tasks()

# --- Main Execution ---
async def main():
    toolbox = ToolBox()
    config_data = toolbox.get_yaml_data()
    pending_tasks = [
        {"subtopic": subtopic, "keyword": keyword.replace('"', ""), "topic": topic}
        for topic, subtopics in config_data.items()
        for subtopic, keyword in subtopics.items()
    ]
    cs = CoroutineSpeedup(task_docker=pending_tasks, power=1)
    await cs.go()

if __name__ == "__main__":
    asyncio.run(main())
