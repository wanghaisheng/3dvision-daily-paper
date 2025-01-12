import requests
import json
from typing import List, Dict, Union

class EnhancedBlogGenerator:
    def __init__(self, api_key: str):
        """
        Initialize the EnhancedBlogGenerator with OpenAI API key.
        """
        self.api_key = api_key
        self.api_url = "https://api.openai.com/v1/chat/completions"
    
    def generate_blog(
        self, 
        prompt: str, 
        model: str = "gpt-4", 
        temperature: float = 0.7, 
        max_tokens: int = 2000
    ) -> str:
        """
        Generate a blog based on the provided prompt.
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        
        data = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        
        response = requests.post(self.api_url, headers=headers, json=data)
        
        if response.status_code == 200:
            response_json = response.json()
            return response_json["choices"][0]["message"]["content"]
        else:
            raise Exception(f"API Error: {response.status_code} - {response.text}")
    
    def batch_generate_blogs(
        self, 
        prompts: List[str], 
        model: str = "gpt-4", 
        temperature: float = 0.7, 
        max_tokens: int = 2000
    ) -> List[str]:
        """
        Generate multiple blogs in a batch based on a list of prompts.
        """
        blogs = []
        for prompt in prompts:
            try:
                blog = self.generate_blog(
                    prompt=prompt,
                    model=model,
                    temperature=temperature,
                    max_tokens=max_tokens
                )
                blogs.append(blog)
            except Exception as e:
                blogs.append(f"Error generating blog for prompt '{prompt}': {e}")
        return blogs
    
    def structured_blog(
        self, 
        prompt: str, 
        sections: List[str], 
        model: str = "gpt-4", 
        temperature: float = 0.7, 
        max_tokens: int = 2000
    ) -> Dict[str, Union[str, None]]:
        """
        Generate a structured blog with specified sections.
        """
        structured_blog = {}
        
        for section in sections:
            try:
                section_prompt = f"{prompt}\nWrite a section on: {section}"
                content = self.generate_blog(
                    prompt=section_prompt,
                    model=model,
                    temperature=temperature,
                    max_tokens=max_tokens
                )
                structured_blog[section] = content
            except Exception as e:
                structured_blog[section] = f"Error: {e}"
        
        return structured_blog

# Example Usage
if __name__ == "__main__":
    # Replace with your actual OpenAI API key
    API_KEY = "your_openai_api_key"
    generator = EnhancedBlogGenerator(api_key=API_KEY)
    
    # Single blog generation
    prompt = "Write a detailed blog post about the benefits of mindfulness meditation."
    try:
        blog = generator.generate_blog(prompt)
        print(blog)
    except Exception as e:
        print(f"Error: {e}")
    
    # Batch blog generation
    prompts = [
        "Write a blog about the importance of exercise.",
        "Write a blog about the basics of personal finance."
    ]
    blogs = generator.batch_generate_blogs(prompts)
    for i, blog in enumerate(blogs):
        print(f"Blog {i+1}:\n{blog}\n")
    
    # Structured blog generation
    structured_prompt = "Write a comprehensive blog about climate change."
    sections = ["Introduction", "Causes", "Effects", "Solutions"]
    structured_blog = generator.structured_blog(prompt=structured_prompt, sections=sections)
    
    for section, content in structured_blog.items():
        print(f"{section}:\n{content}\n")
