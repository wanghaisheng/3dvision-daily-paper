import requests
from datetime import datetime, timedelta
import subprocess
import plotly.figure_factory as ff
import plotly.io as pio
import logging
import os
import networkx as nx
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from collections import Counter, defaultdict
import calendar
import mplcyberpunk
import re
import plotly.express as px
import plotly.graph_objects as go

logging.basicConfig(level=logging.DEBUG)

class EnhancedBlogGenerator:
    def __init__(self, current_date: str, username: str, 
                 api_url: str, api_key: str,
                 model_name: str = "gpt-4",
                 temperature: float = 0.7):
        self.current_date = current_date
        self.username = username
        self.api_url = api_url
        self.api_key = api_key
        self.model_name = model_name
        self.temperature = temperature
    def calculate_reading_time(self,text: str, words_per_minute: int = 200) -> tuple:
        """
        Calculate the estimated reading time for a text.
        
        Args:
            text (str): Content to calculate reading time for
            words_per_minute (int): Reading speed, defaults to 200
        
        Returns:
            tuple: (minutes, seconds)
            
        Example:
            minutes, seconds = calculate_reading_time("Your blog content here")
            print(f"Reading time: {minutes}m {seconds}s")
        """
        try:
            # Remove markdown formatting
            # Remove images
            text = re.sub(r'!\[.*?\]\(.*?\)', '', text)
            # Remove links but keep text
            text = re.sub(r'\[([^\]]+)\]\(.*?\)', r'\1', text)
            # Remove code blocks
            text = re.sub(r'```[\s\S]*?```', '', text)
            # Remove inline code
            text = re.sub(r'`[^`]+`', '', text)
            
            # Count words
            word_count = len(text.split())
            
            # Calculate time
            total_minutes = word_count / words_per_minute
            minutes = int(total_minutes)
            seconds = int((total_minutes - minutes) * 60)
            
            return minutes, seconds
            
        except Exception as e:
            logging.error(f"Error calculating reading time: {e}")
            return 0, 0

    def format_reading_time(self,minutes: int, seconds: int) -> str:
        """
        Format reading time into readable string.
        
        Args:
            minutes (int): Number of minutes
            seconds (int): Number of seconds
        
        Returns:
            str: Formatted time string
            
        Example:
            print(format_reading_time(2, 30))  # "2 minutes 30 seconds"
        """
        try:
            time_parts = []
            
            if minutes > 0:
                time_parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
                
            if seconds > 0:
                time_parts.append(f"{seconds} second{'s' if seconds != 1 else ''}")
                
            if not time_parts:
                return "Less than a minute"
                
            return " ".join(time_parts)
                
        except Exception as e:
            logging.error(f"Error formatting time: {e}")
            return "Unknown reading time"

    def generate_commit_heatmap(self, commits, repo_name, assets_save_folder, assets_read_folder):
        """Generate a GitHub-style commit heatmap."""
        try:
            # Convert commits to dates and count
            dates = [datetime.strptime(commit['date'][:10], '%Y-%m-%d') for commit in commits]
            date_counts = Counter(dates)
            
            # Create date range for the past year
            end_date = max(dates)
            start_date = end_date - timedelta(days=365)
            date_range = pd.date_range(start=start_date, end=end_date)
            
            # Create matrix for heatmap
            weeks = []
            for day in date_range:
                weeks.append([day.weekday(), day.isocalendar()[1], date_counts.get(day.date(), 0)])
            
            df = pd.DataFrame(weeks, columns=['weekday', 'week', 'commits'])
            pivot_table = df.pivot(index='weekday', columns='week', values='commits')
            
            # Create heatmap
            plt.style.use("cyberpunk")
            plt.figure(figsize=(15, 4))
            sns.heatmap(pivot_table, cmap='YlOrRd', linewidths=1)
            
            # Save and return path
            output_path = os.path.join(assets_save_folder, f"{repo_name}-commit_heatmap.png")
            return_path = os.path.join(assets_read_folder, f"{repo_name}-commit_heatmap.png")
            plt.savefig(output_path, bbox_inches='tight', dpi=300)
            plt.close()
            
            return return_path
        except Exception as e:
            logging.error(f"Error generating commit heatmap: {e}")
            return ""

    def generate_contribution_network(self, commits, repo_name, assets_save_folder, assets_read_folder):
        """Generate a network diagram of contributor interactions."""
        try:
            G = nx.Graph()
            
            # Create edges between consecutive commit authors
            for i in range(len(commits)-1):
                author1 = commits[i]['author']
                author2 = commits[i+1]['author']
                if author1 != author2:
                    if G.has_edge(author1, author2):
                        G[author1][author2]['weight'] += 1
                    else:
                        G.add_edge(author1, author2, weight=1)
            
            plt.style.use("cyberpunk")
            plt.figure(figsize=(12, 8))
            
            pos = nx.spring_layout(G)
            nx.draw(G, pos, 
                   with_labels=True,
                   node_color='cyan',
                   node_size=1000,
                   font_size=8,
                   font_weight='bold',
                   edge_color='white',
                   width=[G[u][v]['weight'] for u,v in G.edges()])
            
            output_path = os.path.join(assets_save_folder, f"{repo_name}-contribution_network.png")
            return_path = os.path.join(assets_read_folder, f"{repo_name}-contribution_network.png")
            plt.savefig(output_path, bbox_inches='tight', dpi=300)
            plt.close()
            
            return return_path
        except Exception as e:
            logging.error(f"Error generating contribution network: {e}")
            return ""

    def generate_commit_activity_chart(self, commits, repo_name, assets_save_folder, assets_read_folder):
        """Generate an interactive commit activity chart using Plotly."""
        try:
            # Process commit data
            dates = [datetime.strptime(commit['date'][:19], '%Y-%m-%d %H:%M:%S') for commit in commits]
            hours = [d.hour for d in dates]
            days = [calendar.day_name[d.weekday()] for d in dates]
            
            # Create heatmap data
            activity_matrix = defaultdict(lambda: defaultdict(int))
            for day, hour in zip(days, hours):
                activity_matrix[day][hour] += 1
            
            # Convert to Plotly format
            z_data = []
            days_order = list(calendar.day_name)
            hours_range = list(range(24))
            
            for day in days_order:
                row = []
                for hour in hours_range:
                    row.append(activity_matrix[day][hour])
                z_data.append(row)
            
            fig = go.Figure(data=go.Heatmap(
                z=z_data,
                x=[f"{i:02d}:00" for i in hours_range],
                y=days_order,
                colorscale='Viridis'
            ))
            
            fig.update_layout(
                title='Commit Activity by Day and Hour',
                xaxis_title='Hour of Day',
                yaxis_title='Day of Week',
                template='plotly_dark'
            )
            
            # Save both HTML and image versions
            html_output = os.path.join(assets_save_folder, f"{repo_name}-commit_activity.html")
            img_output = os.path.join(assets_save_folder, f"{repo_name}-commit_activity.png")
            return_path = os.path.join(assets_read_folder, f"{repo_name}-commit_activity.png")
            
            # pio.write_html(fig, html_output)
            pio.write_image(fig, img_output)
            
            return return_path
        except Exception as e:
            logging.error(f"Error generating commit activity chart: {e}")
            return ""

    def generate_code_frequency_chart(self, commits, repo_name, assets_save_folder, assets_read_folder):
        """Generate a chart showing code additions/deletions over time."""
        try:
            # This would require git log with --numstat
            # For demonstration, we'll use commit counts
            dates = [datetime.strptime(commit['date'][:10], '%Y-%m-%d') for commit in commits]
            date_counts = Counter(dates)
            
            df = pd.DataFrame.from_dict(date_counts, orient='index').reset_index()
            df.columns = ['date', 'commits']
            df = df.sort_values('date')
            
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=df['date'],
                y=df['commits'],
                mode='lines',
                name='Commits',
                line=dict(width=2, color='cyan'),
                fill='tozeroy'
            ))
            
            fig.update_layout(
                title='Code Frequency Over Time',
                xaxis_title='Date',
                yaxis_title='Number of Commits',
                template='plotly_dark',
                showlegend=True
            )
            
            output_path = os.path.join(assets_save_folder, f"{repo_name}-code_frequency.png")
            return_path = os.path.join(assets_read_folder, f"{repo_name}-code_frequency.png")
            
            pio.write_image(fig, output_path)
            
            return return_path
        except Exception as e:
            logging.error(f"Error generating code frequency chart: {e}")
            return ""

    
    def _call_api(self, prompt: str, max_tokens: int = 200) -> str:
        """Call the external API with a specific prompt."""
        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            data = {"model": "gpt-4o-mini", "messages": [{"role": "user", "content": prompt}]}
            print('start duckduck===')

            response = requests.post(self.api_url, json=data, headers=headers)
            print('status',response.status_code)
            if response.status_code == 200:
            
                print('response===',response.json())
                response.raise_for_status()
                try:
                    data=response.json()["choices"][0]["message"]["content"].strip()
                    return data
                except:
                    return ''
            else:
                return ''
                
        except Exception as e:
            logging.error(f"API call failed: {e}")
            return ""

    def get_commit_history(self, repo_path: str):
        """Get repository commit history."""
        try:
            cmd = [
                'git', '-C', repo_path, 'log', 
                '--pretty=format:{%n  "hash": "%H",%n  "author": "%an",%n  "date": "%ai",%n  "message": "%s"%n}',
                '--reverse'
            ]
            output = subprocess.check_output(cmd, universal_newlines=True)
            commits = []
            for commit in output.strip().split('\n\n'):
                try:
                    commit_dict = eval(commit)
                    commits.append(commit_dict)
                except Exception as e:
                    logging.warning(f"Error parsing commit: {e}")
                    continue
            return commits
        except Exception as e:
            logging.error(f"Error getting commit history: {e}")
            return []

    
    def generate_timeline_chart(self, commits, repo_name, type='image',assets_read_folder=None,assets_save_folder=None):
        """Generate a timeline visualization of commits."""
        try:
            df = []
            for commit in commits:
                commit_date = datetime.strptime(commit['date'][:19], '%Y-%m-%d %H:%M:%S')
                df.append(dict(
                    Task="Development",
                    Start=commit_date,
                    Finish=commit_date,
                    Description=commit['message'][:30] + '...' if len(commit['message']) > 30 else commit['message']
                ))

            fig = ff.create_gantt(df, 
                                index_col='Description',
                                show_colorbar=True,
                                group_tasks=True,
                                showgrid_x=True,
                                showgrid_y=True)
            
            fig.update_xaxes(
                tickformat="%Y-%m-%d %H:%M:%S",
                tickmode='auto',
                nticks=20
            )

            if type=='html':
                return pio.to_html(fig, full_html=False)
            else:
                # Define assets directory
                
                
                # Create full directory path
                current_dir = os.getcwd()
                full_assets_path = os.path.join(current_dir, assets_save_folder)
                
                # Create assets directory if it doesn't exist
                os.makedirs(full_assets_path, exist_ok=True)
                
                # Create full file path
                output_path = os.path.join(full_assets_path, f"{repo_name}-timeline_chart.png")
                returnoutput_path = os.path.join(assets_read_folder, f"{repo_name}-timeline_chart.png")

                # Save the image
                try:
                    pio.write_image(fig, output_path)
                    logging.info(f"Timeline chart saved as {output_path}")
                    
                    # Verify file was created
                    if os.path.exists(output_path):
                        return returnoutput_path
                    else:
                        logging.error("File was not created successfully")
                        return ""
                except Exception as save_error:
                    logging.error(f"Error saving image: {save_error}")
                    return ""

        except Exception as e:
            logging.error(f"Error generating timeline: {e}")
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
        return self._call_api(prompt, max_tokens=50)

    def generate_blog_post(self, repo_name: str, repo_description: str, readme_content: str, repo_path: str,assets_save_folder:str,assets_read_folder:str) -> str:
        """Generate complete blog post with timeline."""
        print('input to generate',repo_name,repo_description,len(readme_content))
        title = self.generate_title(repo_name, repo_description, readme_content)
        if title.endswith('"'):
            title = title.rstrip('"')
        if title.startswith('"'):
            title = title.lstrip('"')


        
        print('generate_blog_post-generate_title',repo_name,title)
        commits = self.get_commit_history(repo_path)
        
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

Include specific examples and code concepts,wrap code with ```...code...```.
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

        blog_content =''



        for section_name, section_data in sections.items():
            content = self._call_api(section_data['prompt'], max_tokens=1024)
            blog_content += f"{section_data['title']}\n\n{content}\n\n"

        conclusion_prompt = f"""
Write a forward-looking conclusion for {repo_name} that includes:
1. Current project status
2. Future development plans
3. Call to action for contributors
4. Final thoughts on the side project journey

Base it on this README:
{readme_content}
"""
        conclusion = self._call_api(conclusion_prompt, max_tokens=300)
        blog_content += f"## What's Next?\n\n{conclusion}\n"
        minutes, seconds = self.calculate_reading_time(blog_content)
        reading_time = self.format_reading_time(minutes, seconds)

        blog_content = f"""
*Built by {self.username} | Last updated: {self.current_date.split(' ')[0]}*

{reading_time}  read
"""+blog_content
        # Generate all visualizations
        heatmap_path = self.generate_commit_heatmap(commits, repo_name, assets_save_folder, assets_read_folder)
        network_path = self.generate_contribution_network(commits, repo_name, assets_save_folder, assets_read_folder)
        activity_path = self.generate_commit_activity_chart(commits, repo_name, assets_save_folder, assets_read_folder)
        frequency_path = self.generate_code_frequency_chart(commits, repo_name, assets_save_folder, assets_read_folder)
        timeline_path = self.generate_timeline_chart(commits,repo_name,type='image',assets_save_folder=assets_save_folder,assets_read_folder=assets_read_folder)
        
        # Start building the blog content
        blog_content += f"""## Project Development Analytics
### timeline gant

![Commit timelinegant]({timeline_path})


### Commit Activity Heatmap
This heatmap shows the distribution of commits over the past year:

![Commit Heatmap]({heatmap_path})

### Contributor Network
This network diagram shows how different contributors interact:

![Contributor Network]({network_path})

### Commit Activity Patterns
This chart shows when commits typically happen:

![Commit Activity]({activity_path})

### Code Frequency
This chart shows the frequency of code changes over time:

![Code Frequency]({frequency_path})

"""

        

        if blog_content is None:
            blog_content=repo_name+' '+ repo_description
        return blog_content, title
def main():
# Example usage
# Define API URL and Key
  api_url = "https://api.example.com/chat/completions"
  api_key = "your_api_key_here"

  repo_path = "path/to/repo"
  repo_name = "Sample Project"
  repo_description = "A brief description of the project."
  readme_content = "This project is about ..."

  generator = EnhancedBlogGenerator(
    current_date="2024-12-21",
    username="example_user",
    api_url=api_url,
    api_key=api_key
)

  blog_post, title = generator.generate_blog_post(repo_name, repo_description, readme_content, repo_path)
  print(blog_post)
