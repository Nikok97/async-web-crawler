def should_skip_url(currenturl, ctx):
    """
    Determines whether a given URL should be skipped based on several conditions.
    
    Parameters:
    - currenturl: The URL to be checked.
    - ctx: The CrawlerContext which contains rules and configuration.
    - rp: The RobotFileParser object to check robots.txt rules.
    
    Returns:
    - bool: True if the URL should be skipped, otherwise False.
    """
    
    skip_reason = None 

    #Call url_allowed to check regex patterns for inclusion/exclusion
    if not url_allowed(currenturl, ctx.rules["include_paths"], ctx.rules["exclude_regexes"]):
        skip_reason = "URL not allowed by include/exclude ctx.rules"
        print("URL not allowed by include/exclude ctx.rules")

    # --- Check robots.txt ---
    elif not ctx.rules["rp"].can_fetch("*", currenturl):
        skip_reason = "Blocked by robots.txt"
        print("Blocked by robots")

    if skip_reason:
        return True
   
    return False
