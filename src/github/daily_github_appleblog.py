import traceback
import requests
import json
import os
import datetime
import random
import yaml
from dotenv import load_dotenv
import asyncio
from bloghelper import EnhancedBlogGenerator
from default_image_requests import preparedefaultimage
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))

# from default_image import preparedefaultimage

# Load environment variables from .env file
load_dotenv()
print("Script started")

# Load config file for default author, folder, and OpenAI model
CONFIG_FILE = "config.yml"


def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as file:
            return yaml.safe_load(file)
    return {}


config = load_config()
theme = config.get("blogtheme", "appleblog")

days_threshold = config.get("days_threshold", 1)

assets_save_folder = config.get("assets_save_folder", "")
assets_save_folder = os.path.join(project_root, assets_save_folder)

assets_read_folder = config.get("assets_read_folder", "")
domain = config.get("domain", "")


api_url = config.get(
    "OPENAI_API_URL",
    "https://heisenberg-duckduckgo-66.deno.dev/v1/chat/completions",
)
api_model = config.get("api_model", "gpt-4o-mini")

api_key = os.getenv("OPENAI_API_KEY", "your_self_openai_api_access_token_here")

TAGS_SERVER_DIR_STORAG = config.get("tag_file_path", "")
TAGS_SERVER_DIR_STORAG = os.path.join(project_root, TAGS_SERVER_DIR_STORAG)

DEFAULT_AUTHOR_LIST = config.get("author_list", ["unknown"])
OUTPUT_FOLDER = config.get("output_folder", "markdown_files")
OUTPUT_FOLDER = os.path.join(project_root, OUTPUT_FOLDER)

IMAGE_FOLDER = config.get(
    "image_folder", "generated_images"
)  # Image folder to save images
BASE_URL = config.get(
    "base_url", "http://localhost:3000/"
)  # Base URL for generated images
IMAGE_API_URL = config.get("image_api_url", "image_api_url")
FLUX_MODEL = config.get("flux_model", "DS-8-CF")

print("yml config", config)
# Ensure the output folder exists
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

if not os.path.exists(IMAGE_FOLDER):
    os.makedirs(IMAGE_FOLDER)

# GitHub API Token
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "your_github_access_token_here")
SILICON_TOKEN = os.getenv(
    "SILICON_TOKEN", "your_SILICON_TOKEN_access_token_here"
)

print("your token", GITHUB_TOKEN)
HEADERS = {"Authorization": f"token {GITHUB_TOKEN}"}

# Image generation API URL and Token
IMAGE_API_KEY = os.getenv("IMAGE_API_KEY", "your_image_api_key_here")

print("your IMAGE_API_KEY", IMAGE_API_KEY)

import re

defaultimages = preparedefaultimage()


def replace_non_word_characters(input_list):
    # This function takes a list of strings and replaces non-word characters with '-'
    converted_list = []
    for item in input_list:
        # Replace non-word characters with '-'
        item = item.strip()
        item = item.lower()
        new_item = re.sub(r"\W", "-", item)
        if len(new_item) > 100:
            new_item = new_item[:100]
        new_item = new_item.replace("_", "-")

        if new_item and new_item != "-":
            converted_list.append(new_item)
    return converted_list


# Example usage
# input_list = ['apple123!', 'banana@#', 'cherry_456']
# result = replace_non_word_characters(input_list)


# Fetch all repositories of a user
def get_repositories(username):
    url = f"https://api.github.com/users/{username}/repos"
    repos = []
    params = {"per_page": 100, "page": 1}

    while True:
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200:
            print(
                f"Failed to fetch repos: {response.json().get('message', 'Unknown error')}"
            )
            break

        data = response.json()
        if not data:
            break

        repos.extend(data)
        params["page"] += 1

    return repos


# Fetch README content
def get_readme_content(owner, repo):
    url = f"https://api.github.com/repos/{owner}/{repo}/readme"
    response = requests.get(url, headers=HEADERS)

    if response.status_code == 200:
        readme_data = response.json()
        readme_content = requests.get(readme_data["download_url"]).text
        return readme_content
    return None


def openai_api_call( prompt, model="gpt-4o-mini", retries=3, delay=5):
    # Set the endpoint URL and headers
    urls = [
        "https://heisenberg-duckduckgo-12.deno.dev/v1/chat/completions",
        "https://heisenberg-duckduckgo-66.deno.dev/v1/chat/completions",
        "https://heisenberg-duckduckgo-38.deno.dev/v1/chat/completions",
    ]
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    # Define the data payload
    # Modified
    data = {
        "model": api_model,
        "messages": [{"role": "user", "content": prompt}],
    }
    for attempt in range(retries):
        url = urls[attempt]

        try:
            response = requests.post(url, headers=headers, json=data)

            # Check for a successful response
            if response.status_code == 200:
                data = response.json()
                result = data["choices"][0]["message"]["content"]
                return result
            else:
                print(
                    f"Request {url} failed with status code  {response.status_code}: {response.text}"
                )

        except Exception as e:
            print("Error call openai api endpoint", e)

        if attempt < retries - 1:
            print(f"Retrying in {delay} seconds...")
            time.sleep(delay)


def siliconflow(text, token, model="Qwen2.5"):

    payload = {
        "model": "Qwen/Qwen2.5-7B-Instruct",
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": text}]}
        ],
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    response = requests.request("POST", api_url, json=payload, headers=headers)

    print(response.text)
    data = response.json()
    if data:
        try:
            result = data["choices"][0]["message"]["content"]
            return result
        except:
            return None
    return None


# Extract keywords and tags using Chat class
async def extract_keywords_and_tags(text):
    text = text[:3000]
    prompt = f"Extract keywords from the following text:\n{text}\n, return keywords as comma separator:"
    keywords_response = openai_api_call(prompt=prompt)
    print("---------generated keywords", keywords_response)

    keywords = (
        keywords_response.split(":")[1]
        if ":" in keywords_response
        else keywords_response
    )
    keywords = keywords.split(",")

    prompt = f"Extract tags from the following text:\n{text}\n, return tags as comma separator"
    tags_response = openai_api_call(prompt=prompt)
    print("---------generated tags", tags_response)
    tags_response = (
        tags_response.split(":")[1] if ":" in tags_response else tags_response
    )
    if "\n" in tags_response:
        tags_response = tags_response.split("\n")

        tags_response = ",".join(tags_response)
    tags = tags_response.split(",") if "," in tags_response else tags_response
    print("---------generated tags", tags_response)

    tags = replace_non_word_characters(tags)
    if not isinstance(tags, list):

        print("---------generated tags is not list", tags)

    return keywords, tags


# Select a random author from the list
def select_author():
    return random.choice(DEFAULT_AUTHOR_LIST)


# Check if the repository was recently updated
def is_recently_updated(repo, days_threshold=30):
    last_updated = datetime.datetime.strptime(
        repo["pushed_at"], "%Y-%m-%dT%H:%M:%SZ"
    )
    today = datetime.datetime.utcnow()
    delta = today - last_updated
    return delta.days <= days_threshold


# Save the generated image locally
def save_image_locally(image_data, image_name):
    image_path = os.path.join(IMAGE_FOLDER, image_name)
    with open(image_path, "wb") as image_file:
        image_file.write(image_data)
    return image_path


# Generate a cover image based on the prompt
def call_image_endpoint_local(api_url, api_key, prompt, size="1024x1024", n=1):
    prompt = prompt[:1000]
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    data = {
        "messages": [{"role": "user", "content": prompt}],
        "model": FLUX_MODEL,
    }
    response = requests.post(api_url, headers=headers, data=json.dumps(data))

    if response.status_code == 200:
        result = response.json()
        print(f"Original Prompt: {result['prompt']}")
        print(f"Translated Prompt: {result['translatedPrompt']}")
        image_data = result.get("image", "")
        if image_data:
            # Save the image locally
            image_name = (
                f"{prompt[:10]}.png"  # Save with a short name based on prompt
            )
            # image_path = save_image_locally(image_data.encode(), image_name)
            # if local save
            image_url = domain + assets_read_folder + image_name
            # image_url = BASE_URL + IMAGE_FOLDER + "/" + image_name  # URL to access the image
            return image_url
        else:
            print("No image data received.")
    else:
        print(f"Failed to generate cover image: {response.status_code}")
    return None


def save_image_from_url(image_url, local_file_path):
    # Send a GET request to the image URL
    response = requests.get(image_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Open a local file in binary write mode
        with open(local_file_path, "wb") as file:
            # Write the content of the response (the image) to the file
            file.write(response.content)
        print(f"Image saved successfully to {local_file_path}")
    else:
        print(f"Failed to retrieve image. Status code: {response.status_code}")


def call_image_endpoint(api_url, api_key, prompt, size="1024x1024", n=1):
    """
    Calls the Cloudflare Worker image generation endpoint.

    Args:
        api_url (str): The endpoint URL of the Cloudflare Worker.
        api_key (str): The API key for authentication.
        prompt (str): The text prompt to generate the image.
        size (str, optional): The size of the generated image (e.g., "1024x1024"). Defaults to "1024x1024".
        n (int, optional): The number of images to generate. Defaults to 1.

    Returns:
        dict: The JSON response from the endpoint.
    """
    prompt = prompt[:1000]
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
    }
    image_url = None
    try:
        response = requests.post(api_url, json=payload, headers=headers)

        if response.status_code == 200:
            data = response.json()
            if data:
                try:
                    url = data["choices"][0]["message"]["url"]
                    print("create image", url)
                    image_name = url.split("/")[-1] + ".png"
                    assets_save_folder = os.path.join(project_root, assets_save_folder)

                    local_file_path = os.path.join(
                        assets_save_folder, image_name
                    )
                    save_image_from_url(url, local_file_path)

                    image_url = domain + assets_read_folder + image_name

                except:
                    image_name = random.choice(defaultimages)
        else:
            print("error:\n", response.status_code, "message:\n", response.text)
    except Exception as e:
        print("error image creation", e)
    if image_url is None:
        image_name = random.choice(defaultimages)
        image_url = domain + assets_read_folder + image_name


def generate_blog(
    repo_name,
    repo_description,
    readme_content,
    username=None,
    current_date=None,
    assets_read_folder=None,
    assets_save_folder=None,
):
    if current_date is None:
        current_date = datetime.datetime.now().strftime("%Y%m%d %H%M%S")

    if username is None:
        username = "wanghaisheng"
    generator = EnhancedBlogGenerator(
        current_date=current_date,
        username=username,
        api_url=api_url,
        api_key=api_key,
        model_name="gpt-4",
        temperature=0.7,
    )
    blog_post, title = generator.generate_blog_post(
        repo_name=repo_name,
        repo_description=repo_description,
        readme_content=readme_content,
        repo_path=".",
        assets_save_folder=assets_save_folder,
        assets_read_folder=assets_read_folder,
    )
    print("generate title", title)
    return blog_post, title


# Create Markdown file for each repository
def update_apple_blog_tags_json(tags):
    new = {}
    # tags=replace_non_word_characters(tags)

    if os.path.exists(TAGS_SERVER_DIR_STORAG):
        print("overwrite old tags.json")
        new = []
        try:
            old = json.load(open(TAGS_SERVER_DIR_STORAG), encoding="utf8").get(
                "tags", []
            )
            new = old + list(set(tags))
        except Exception as e:
            print("old tag json error", e)
            new = tags
        new = list(set(new))
        tags = replace_non_word_characters(tags)
        with open(TAGS_SERVER_DIR_STORAG, "w", encoding="utf-8") as file:
            json.dump({"tags": new}, file, ensure_ascii=False, indent=2)
            print("save tag json file")
    else:
        print("create new tags.json")
        new = list(set(tags))
        tags = replace_non_word_characters(tags)

        os.makedirs(os.path.dirname(TAGS_SERVER_DIR_STORAG), exist_ok=True)

        with open(TAGS_SERVER_DIR_STORAG, "w", encoding="utf-8") as file:
            json.dump({"tags": new}, file, ensure_ascii=False, indent=2)


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


def create_all_markdown_files(repos, username, chat, days_threshold=30):
    date_today = datetime.date.today().strftime("%Y-%m-%d")
    pubdate = datetime.datetime.now().strftime("%Y%m%d %H%M%S")

    for repo in repos:
        # Skip repositories that haven't been updated recently
        if not is_recently_updated(repo, days_threshold):
            print(f"Skipping {repo['name']} (not updated recently).")
            continue

        repo_name = repo["name"]
        repo_url = repo["html_url"]
        stars_count = repo["stargazers_count"]
        forks_count = repo["forks_count"]
        description = repo["description"] or "No description provided."
        is_forked = repo["fork"]

        # Fetch README content or fallback to description
        readme_content = get_readme_content(username, repo_name)
        blogmd, title = generate_blog(
            repo_name,
            repo_description=description,
            readme_content=readme_content,
            username=username,
            current_date=None,
            assets_save_folder=assets_save_folder,
            assets_read_folder=assets_read_folder,
        )
        if title is None:
            title = repo_name

        # Generate cover image based on the description or repository name
        cover_image_url = call_image_endpoint(
            api_url=IMAGE_API_URL,
            api_key=IMAGE_API_KEY,
            prompt=f"A creative image representing the repository: {readme_content}",
        )

        # Extract keywords and tags using Chat class
        keywords, tags = asyncio.run(
            extract_keywords_and_tags(
                chat, f"{repo_name} {description} {readme_content}"
            )
        )
        if theme == "appleblog":
            update_apple_blog_tags_json(tags)

        # Select author
        author = select_author()

        # Construct Markdown content
        if theme == "appleblog":
            frontmatter = build_frontmatter_appleblog(
                author=author,
                cover_image_url=cover_image_url,
                description=description,
                keywords=", ".join(keywords),
                pubdate=date_today,
                tags=tags,
                title=title,
            )

        md_content = f"""
        ---\n
        {frontmatter}
        ---\n
{blogmd}

### Repository URL: 
[{repo_url}]({repo_url})
### Stars: 
**{stars_count}**
### Forks: 
**{forks_count}**
"""

        # Save to .md file in the output folder
        
        filename = os.path.join(OUTPUT_FOLDER, f"{repo_name}.md")
        with open(filename, "w", encoding="utf-8") as file:
            file.write(md_content)
        print(f"Markdown file created: {filename}")


# 你想多久运行一次程序就设置时间间隔为多久 一个月 一周 一天
async def create_new_markdown_files(repos, username, days_threshold=1):
    date_today = datetime.date.today().strftime("%Y-%m-%d")
    pubdate = datetime.datetime.now().strftime("%Y%m%d %H%M%S")

    for repo in repos:
        # Skip repositories that haven't been updated recently
        if not is_recently_updated(repo, days_threshold):
            print(f"Skipping {repo['name']} (not updated recently).")
            continue

        repo_name = repo["name"]
        repo_url = repo["html_url"]
        stars_count = repo["stargazers_count"]
        forks_count = repo["forks_count"]
        description = repo["description"] or "No description provided."
        is_forked = repo["fork"]

        # Check if markdown file already exists for this repository
        md_filename = os.path.join(OUTPUT_FOLDER, f"{repo_name}.md")
        if os.path.exists(md_filename):
            print(f"Skipping {repo_name} (markdown file already exists).")
            continue

        # Fetch README content or fallback to description
        readme_content = get_readme_content(username, repo_name) or description
        blogmd, title = generate_blog(
            repo_name,
            repo_description=description,
            readme_content=readme_content,
            username=username,
            current_date=None,
            assets_save_folder=assets_save_folder,
            assets_read_folder=assets_read_folder,
        )
        if title is None:
            title = repo_name

        # Generate cover image based on the description or repository name
        cover_image_url = call_image_endpoint(
            api_url=IMAGE_API_URL,
            api_key=IMAGE_API_KEY,
            prompt=f"A creative image representing the repository: {readme_content}",
        )

        # Extract keywords and tags using Chat class
        keywords, tags = await extract_keywords_and_tags(
            f"{repo_name} {description} {readme_content}"
        )
        if theme == "appleblog":
            update_apple_blog_tags_json(tags)

        # Select author
        author = select_author()

        # Construct Markdown content
        if theme == "appleblog":
            frontmatter = build_frontmatter_appleblog(
                author=author,
                cover_image_url=cover_image_url,
                description=description,
                keywords=", ".join(keywords),
                pubdate=date_today,
                tags=tags,
                title=title,
            )

        md_content = f"""---\n{frontmatter}---\n

{blogmd}

* Repository URL: [{repo_url}]({repo_url})
* Stars: **{stars_count}**
* Forks: **{forks_count}**
"""

        # Save to .md file in the output folder
        with open(md_filename, "w", encoding="utf-8") as file:
            file.write(md_content)
        print(f"Markdown file created: {md_filename}")


# Main execution
async def main():
    try:
        print("main function start")
        # Initialize the chat with the model from config
        # Get the username from config
        username = config.get("username", "default_username")
        print("start to detect all repos")
        repos = get_repositories(username)[:20]
        if not repos:
            print("No repositories found or failed to fetch repositories.")
            return
        print("create md for repos")
        await create_new_markdown_files(
            repos, username, days_threshold=days_threshold
        )
    except Exception as e:
        print(f"Exception in main: {e}")
        traceback.print_exc()


# Run the async main function
if __name__ == "__main__":
    print("we are coming")
    asyncio.run(main())
