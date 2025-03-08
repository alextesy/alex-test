# Stock Market Scraper Firebase Function

This Firebase Cloud Function scrapes Reddit and Twitter for stock market-related discussions and stores them in a SQL database.

## Prerequisites

1. Firebase CLI installed
2. Firebase project created
3. SQL database set up and accessible
4. Reddit API credentials
5. Twitter API credentials (if using Twitter scraping)

## Environment Variables

Create a `.env` file in the functions directory with the following variables:

```env
# Database
DATABASE_URL=postgresql://user:password@host:port/dbname

# Reddit API
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_USER_AGENT=your_user_agent

# Twitter API (if using Twitter scraping)
TWITTER_API_KEY=your_api_key
TWITTER_API_SECRET=your_api_secret
TWITTER_ACCESS_TOKEN=your_access_token
TWITTER_ACCESS_TOKEN_SECRET=your_access_token_secret
```

## Deployment

1. Initialize Firebase in your project if not already done:
   ```bash
   firebase init
   ```

2. Select "Functions" when prompted for features to set up.

3. Deploy the function:
   ```bash
   firebase deploy --only functions
   ```

## Usage

The function can be triggered via HTTP request. It accepts the following query parameters:

- `enable_twitter`: Set to 'true' to enable Twitter scraping (optional, defaults to false)

Example:
```bash
curl "https://your-region-your-project.cloudfunctions.net/run_scraper?enable_twitter=true"
```

## State Management

The function uses Firebase Firestore to store state between runs:

- Collection: `scraper_state`
- Document: `reddit`
  - Field: `last_daily_discussion_id` - ID of the last processed Reddit discussion thread
  - Field: `last_updated` - Timestamp of the last update

## Response Format

Success response:
```json
{
    "status": "success",
    "reddit_count": 123,
    "twitter_count": 45
}
```

Error response:
```json
{
    "status": "error",
    "message": "Error description"
}
``` 