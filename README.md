# datacore-board-assessment
 
Technical Challenges - Task 2 (Vietstock):

    Challenge: Encountered latin-1 codec errors during the POST request to the getlistceo endpoint.

    Analysis: The __RequestVerificationToken provided by Vietstock contained non-ASCII characters that are incompatible with Python's default http.client header validation.

    Mitigation Attempted: Implemented per-ticker session clearing, manual header string casting, and randomized delays (safe_sleep) to mimic human behavior.

    Observation: Determined that consistent 404s and Codec errors were likely an IP-based firewall response (e.g., Cloudflare) serving junk tokens to automated requests.