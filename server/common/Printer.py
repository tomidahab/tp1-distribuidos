class Printer:
    """
    A utility class for printing colored text to the console.
    Uses ANSI color codes for text formatting.
    """
    
    # ANSI color code constants
    COLORS = {
        "red": "\033[91m",
        "green": "\033[92m",
        "yellow": "\033[93m",
        "blue": "\033[94m",
        "purple": "\033[95m",
        "cyan": "\033[96m",
        "white": "\033[97m",
        "reset": "\033[0m"
    }
    
    @classmethod
    def colored_text(cls, text, color):
        """
        Return text with the specified color using ANSI color codes.
        
        Args:
            text (str): The text to be colored
            color (str): The color to use (red, green, yellow, blue, etc.)
            
        Returns:
            str: The colored text string
        """
        if color.lower() not in cls.COLORS:
            return text  # Return original text if color not found
            
        return f"{cls.COLORS[color.lower()]}{text}{cls.COLORS['reset']}"
    
    @classmethod
    def red(cls, text):
        return cls.colored_text(text, "red")
    
    @classmethod
    def green(cls, text):
        return cls.colored_text(text, "green")
        
    @classmethod
    def yellow(cls, text):
        return cls.colored_text(text, "yellow")
        
    @classmethod
    def blue(cls, text):
        return cls.colored_text(text, "blue")
        
    @classmethod
    def purple(cls, text):
        return cls.colored_text(text, "purple")
        
    @classmethod
    def cyan(cls, text):
        return cls.colored_text(text, "cyan")