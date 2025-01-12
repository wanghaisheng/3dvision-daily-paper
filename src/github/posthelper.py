import json
import os
from typing import Dict, List, Optional
from datetime import datetime

class PostPromptHelper:
    def __init__(self, template_path: str, template_type: str = "saas"):
        """
        Initialize the PostPromptHelper with templates from a JSON file.
        
        Args:
            template_path: Path to the JSON template file
            template_type: Type of templates to use ("saas", "game", "ecommerce")
        """
        self.template_type = template_type
        self.templates = self._load_templates(template_path)
        self.template_root = self._get_template_root()
        
    def _load_templates(self, template_path: str) -> Dict:
        """Load templates from JSON file."""
        if not os.path.exists(template_path):
            raise FileNotFoundError(f"Template file not found: {template_path}")
            
        with open(template_path, 'r', encoding='utf-8') as f:
            return json.load(f)
            
    def _get_template_root(self) -> Dict:
        """Get the appropriate template root based on template type."""
        if self.template_type == "saas":
            return self.templates.get("post_templates", {})
        elif self.template_type == "game":
            return self.templates.get("game_social_templates", {})
        elif self.template_type == "ecommerce":
            return self.templates.get("ecommerce_templates", {})
        else:
            raise ValueError(f"Unknown template type: {self.template_type}")

    def get_platform_guidelines(self, platform: str) -> Dict:
        """
        Get platform-specific posting guidelines.
        
        Args:
            platform: The social media platform (e.g., "twitter", "instagram")
            
        Returns:
            Dict containing platform guidelines
        """
        return self.templates.get("post_rules", {}).get(platform, {})

    def validate_context_vars(self, stage: str, category: str, post_type: str, context_vars: Dict) -> bool:
        """
        Validate that all required variables are present in context_vars.
        
        Args:
            stage: The customer journey stage
            category: The category of post (e.g., "product_introduction", "feature_spotlight")
            post_type: The specific type of post
            context_vars: The variables provided for the template
            
        Returns:
            bool: True if all required variables are present
        """
        template = self.template_root.get(stage, {}).get(category, {}).get(post_type)
        if not template:
            raise ValueError(f"Unknown template path: {stage}/{category}/{post_type}")
            
        required_vars = set(template.get("required_vars", {}).keys())
        provided_vars = set(context_vars.keys())
        
        missing_vars = required_vars - provided_vars
        if missing_vars:
            raise ValueError(f"Missing required variables: {missing_vars}")
            
        return True
        
    def generate_post(self, stage: str, category: str, post_type: str, 
                     context_vars: Dict, platform: str = None) -> Dict:
        """
        Generate a social media post based on the template.
        
        Args:
            stage: The customer journey stage
            category: The category of post
            post_type: The specific type of post
            context_vars: Variables to fill in the template
            platform: Optional - specific platform to generate for
            
        Returns:
            Dict containing the generated post content and metadata
        """
        # Validate inputs
        self.validate_context_vars(stage, category, post_type, context_vars)
        
        # Get template
        template = self.template_root[stage][category][post_type]
        
        # Get platform-specific guidelines if needed
        platform_guidelines = self.get_platform_guidelines(platform) if platform else {}
        
        # Format template with context variables
        formatted_template = template["template"].format(**context_vars)
        
        # Include hashtags based on template type and stage
        hashtags = self.templates.get("platform_guidelines", {}).get("hashtag_strategy", {}).get(stage, [])
        
        return {
            "content": formatted_template,
            "hashtags": hashtags,
            "platform_guidelines": platform_guidelines,
            "metadata": {
                "template_type": self.template_type,
                "stage": stage,
                "category": category,
                "post_type": post_type,
                "generated_at": datetime.utcnow().isoformat()
            }
        }

    def get_template_structure(self) -> Dict:
        """
        Get the available template structure for the current template type.
        
        Returns:
            Dict containing available stages, categories, and post types
        """
        structure = {}
        for stage in self.template_root:
            structure[stage] = {}
            for category in self.template_root[stage]:
                structure[stage][category] = list(self.template_root[stage][category].keys())
        return structure

# Example usage
if __name__ == "__main__":
    # Initialize helper with template file
    helper = PostPromptHelper("social_media_templates.json", "saas")
    
    # Example context variables
    context_vars = {
        "product_name": "GitHub Daily Event to Blog Update",
        "core_features": ["Activity tracking", "Blog generation", "Automated updates"],
        "problem": "Manual activity tracking",
        "solution": "Automated GitHub activity summarization"
    }
    
    try:
        # Generate an awareness stage product introduction post
        post = helper.generate_post(
            stage="awareness_stage",
            category="product_introduction",
            post_type="what_is_product",
            context_vars=context_vars,
            platform="twitter"
        )
        
        print("Generated Post:")
        print("-" * 50)
        print("Content:")
        print(post['content'])
        print("\nHashtags:")
        print(", ".join(post['hashtags']))
        print("\nPlatform Guidelines:")
        print(json.dumps(post['platform_guidelines'], indent=2))
        print("\nMetadata:")
        print(json.dumps(post['metadata'], indent=2))
        
    except Exception as e:
        print(f"Error generating post: {str(e)}")
