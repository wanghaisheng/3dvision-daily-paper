from datetime import datetime
from typing import Dict, Optional, List
import g4f
from g4f.client import Client
from g4f.Provider import BaseProvider
import time
import subprocess
import plotly.figure_factory as ff
import plotly.io as pio
import base64
from io import BytesIO
import logging

logging.basicConfig(level=logging.DEBUG)

class EnhancedBlogGenerator:
    def __init__(self, current_date: str, username: str, 
                 provider_name: str = "g4f.Provider.Bing",
                 model_name: str = "gpt-4",
                 temperature: float = 0.7):
        self.current_date = current_date
        self.username = username
        self.provider_name = provider_name
        self.model_name = model_name
        self.temperature = temperature
        self.client = self._setup_client()

    def _setup_client(self) -> Client:
        """Setup g4f client with specified provider."""
        logging.debug("Setting up g4f client...")

        provider_class = self._get_provider_class(self.provider_name)
        logging.debug(f"Provider class resolved: {provider_class}")
        
        return Client(provider=provider_class)

    def _get_provider_class(self, provider_name: str) -> Optional[BaseProvider]:
        """Get the provider class from g4f."""
        if provider_name == 'auto':
            return None
        try:
            provider_class = getattr(g4f.Provider, provider_name.split('.')[-1])
            if not issubclass(provider_class, BaseProvider):
                raise ValueError(f"Invalid provider: {provider_name}")
            return provider_class
        except AttributeError:
            raise ValueError(f"Provider not found: {provider_name}")

    def get_commit_history(self, repo_path: str) -> List[Dict]:
        """Get repository commit history."""
        try:
            # Get git log with formatting
            cmd = [
                'git', '-C', repo_path, 'log', 
                '--pretty=format:{%n  "hash": "%H",%n  "author": "%an",%n  "date": "%ai",%n  "message": "%s"%n}',
                '--reverse'
            ]
            output = subprocess.check_output(cmd, universal_newlines=True)
            
            # Parse the JSON-like output
            commits = []
            for commit in output.strip().split('\n\n'):
                try:
                    commit_dict = eval(commit)
                    commits.append(commit_dict)
                except:
                    continue
                    
            return commits
        except Exception as e:
            print(f"Error getting commit history: {e}")
            return []

    def generate_timeline_chart(self, commits: List[Dict]) -> str:
        """Generate a timeline visualization of commits."""
        try:
            # Prepare data for timeline
            df = []
            for commit in commits:
                df.append(dict(
                    Task="Development",
                    Start=datetime.strptime(commit['date'][:19], '%Y-%m-%d %H:%M:%S'),
                    Finish=datetime.strptime(commit['date'][:19], '%Y-%m-%d %H:%M:%S'),
                    Description=commit['message'][:30] + '...' if len(commit['message']) > 30 else commit['message']
                ))

            # Create timeline
            fig = ff.create_gantt(df, 
                                index_col='Description',
                                show_colorbar=True,
                                group_tasks=True,
                                showgrid_x=True,
                                showgrid_y=True)

            # Save to HTML string
            timeline_html = pio.to_html(fig, full_html=False)
            
            return timeline_html
        except Exception as e:
            print(f"Error generating timeline: {e}")
            return ""

    def generate_title(self, repo_name: str, repo_description: str, readme_content: str) -> str:
        """Generate an engaging blog title."""
        prompt = f"""
Generate an engaging, creative blog title for a developer side project.
Project details:
- Name: {repo_name}
- Description: {repo_description}
- README excerpt: {readme_content[:500]}...

Requirements:
1. Use one of these formats:
   - "From Idea to Reality: [Project Name]"
   - "Building [X]: A Developer's Side Project Journey"
   - "Weekend Hack: How I Built [X]"
   - "[Solving X] with [Technology]: A Side Project Story"
2. Make it engaging and specific
3. Include relevant tech keywords
4. Keep it under 80 characters
5. Focus on the building/creation aspect

Return ONLY the title, nothing else.
"""
        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {'role': 'system', 'content': 'You are a technical blog title generator.'},
                    {'role': 'user', 'content': prompt}
                ],
                temperature=0.8,
                max_tokens=50
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"Error generating title: {e}")
            return f"Building {repo_name}: A Developer's Journey"

    def generate_blog_post(self, repo_name: str, repo_description: str, 
                          readme_content: str, repo_path: str) -> str:
        """Generate complete blog post with timeline."""
        # Generate engaging title
        title = self.generate_title(repo_name, repo_description, readme_content)

        # Get commit history and generate timeline
        commits = self.get_commit_history(repo_path)
        timeline_html = self.generate_timeline_chart(commits)

        sections = {
            'introduction': {
                'title': '## Project Genesis',
                'prompt': f"""
Write an engaging introduction for a blog post about {repo_name}.
Context:
{readme_content[:500]}...

Include:
1. The spark/inspiration for the project
2. Personal motivation
3. Initial challenges
4. Quick overview of the solution

Write in first person, make it personal and engaging.
"""
            },
            'research': {
                'title': '## From Idea to Implementation',
                'prompt': f"""
Based on the repository README:
{readme_content}

Write about:
1. Initial research and planning
2. Technical decisions and their rationale
3. Alternative approaches considered
4. Key insights that shaped the project

Focus on the journey from concept to code.
"""
            },
            'technical': {
                'title': '## Under the Hood',
                'prompt': f"""
Analyze this README content:
{readme_content}

Create a technical deep-dive covering:
1. Architecture decisions
2. Key technologies used
3. Interesting implementation details
4. Technical challenges overcome

Include specific examples and code concepts.
"""
            },
            'lessons': {
                'title': '## Lessons from the Trenches',
                'prompt': f"""
Based on the project history and README:
{readme_content}

Share:
1. Key technical lessons learned
2. What worked well
3. What you'd do differently
4. Advice for others

Be specific and practical.
"""
            }
        }

        # Generate blog content
        blog_content = f"""# {title}

*Built by {self.username} | Last updated: {self.current_date}*

"""
        # Add project timeline
        if timeline_html:
            blog_content += """
## Project Timeline

<div class="timeline-container">
"""
            blog_content += timeline_html
            blog_content += """
</div>

"""

        # Generate each section
        for section_name, section_data in sections.items():
            print(f"Generating {section_name} section...")
            blog_content += f"{section_data['title']}\n\n"
            content = self.generate_section_content(section_name, section_data['prompt'])
            blog_content += f"{content}\n\n"

        # Add conclusion with future plans
        conclusion_prompt = f"""
Write a forward-looking conclusion for {repo_name} that includes:
1. Current project status
2. Future development plans
3. Call to action for contributors
4. Final thoughts on the side project journey

Base it on this README:
{readme_content}
"""
        conclusion = self.generate_section_content('conclusion', conclusion_prompt)
        blog_content += f"## What's Next?\n\n{conclusion}\n"

        return blog_content,title

    def generate_section_content(self, section_name: str, prompt: str, 
                               max_retries: int = 3) -> str:
        """Generate content for a specific section using g4f."""
        retry_count = 0
        while retry_count < max_retries:
            try:
                messages = [
                    {
                        'role': 'system',
                        'content': 'You are a technical writer creating engaging content for developer blog posts about side projects.'
                    },
                    {
                        'role': 'user',
                        'content': prompt
                    }
                ]
                
                chat_completion = self.client.chat.completions.create(
                    model=self.model_name,
                    messages=messages,
                    temperature=self.temperature,
                    max_tokens=2048
                )
                
                content = chat_completion.choices[0].message.content.strip()
                if content:
                    return content
                    
            except Exception as e:
                print(f"Error generating content (attempt {retry_count + 1}): {str(e)}")
                retry_count += 1
                time.sleep(2)
                
        return f"[Failed to generate content for {section_name} after {max_retries} attempts]"

def main():
    # Example usage
    print('blog example')
    repo_path = "path/to/your/repo"  # Local repository path
    repo_name = "github-journey-daily-update"
    repo_description = "Track and analyze GitHub activity across repositories"
    
    # Read README content
    try:
        with open(f"{repo_path}/README.md", 'r', encoding='utf-8') as f:
            readme_content = f.read()
    except Exception as e:
        print(f"Error reading README: {e}")
        readme_content = ""

    # Initialize generator
    generator = EnhancedBlogGenerator(
        current_date="2024-12-21 12:48:41",
        username="wanghaisheng",
        provider_name="g4f.Provider.Bing",
        model_name="gpt-4",
        temperature=0.7
    )

    # Generate blog post
    blog_post,title = generator.generate_blog_post(
        repo_name=repo_name,
        repo_description=repo_description,
        readme_content=readme_content,
        repo_path=repo_path
    )

    # Save output
    output_filename = f"blog_post_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
