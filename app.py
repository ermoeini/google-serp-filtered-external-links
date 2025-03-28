import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import pandas as pd
import streamlit as st
from fake_useragent import UserAgent
import requests_cache

# Initialize cache for requests
requests_cache.install_cache(expire_after=3600)

# Generate a user agent once and reuse it
user_agent = UserAgent().random

async def fetch_with_retries(url, session, retries=3, backoff_factor=2):
    """Fetch a URL with retries and exponential backoff."""
    for attempt in range(retries):
        try:
            async with session.get(url, headers={"User-Agent": user_agent}) as response:
                response.raise_for_status()
                return await response.text()
        except aiohttp.ClientError:
            if attempt < retries - 1:
                await asyncio.sleep(backoff_factor ** attempt)
            else:
                raise

async def scrape_google_search_results(query, num_results, lang, exclude_websites, session):
    """Scrape Google search results asynchronously."""
    result_urls = []
    url = f"https://www.google.com/search?q={query}&num={num_results}&gl={lang}"
    
    try:
        html_content = await fetch_with_retries(url, session)
        soup = BeautifulSoup(html_content, "html.parser")
        search_results = soup.find_all("div", class_="tF2Cxc")
        for result in search_results:
            link = result.find("a")["href"]
            parsed_link = urlparse(link)
            if not any(website in parsed_link.geturl() for website in exclude_websites):
                result_urls.append(link)
            else:
                print(f"Excluded: {parsed_link.geturl()}")
    except Exception as e:
        st.error(f"Failed to fetch data: {e}")
        return []
    return result_urls

async def get_external_links(url, session, search_result_domains):
    """Fetch external links from a URL."""
    external_links = []
    try:
        html_content = await fetch_with_retries(url, session)
        soup = BeautifulSoup(html_content, "html.parser")
        all_links = soup.find_all("a")
        for link in all_links:
            href = link.get("href")
            if href and "http" in href:
                parsed_href = urlparse(href)
                parsed_website = urlparse(url)
                if parsed_href.netloc != parsed_website.netloc and parsed_href.netloc in search_result_domains:
                    external_links.append(parsed_href.geturl())
    except Exception as e:
        print(f"An unexpected error occurred while fetching external links for {url}: {e}")
    return external_links

async def process_search_results(urls, session, concurrency_limit=5):
    """Process search results to fetch external links."""
    semaphore = asyncio.Semaphore(concurrency_limit)
    search_result_domains = {urlparse(url).netloc for url in urls}

    async def process_url(url):
        async with semaphore:
            return await get_external_links(url, session, search_result_domains)

    tasks = [process_url(url) for url in urls]
    return await asyncio.gather(*tasks)

async def main_async():
    st.title("Filtered external link extractor From Google serp")
    st.write("Enter your search query and preferences below:")

    # Initialize session_state
    if "run_clicked" not in st.session_state:
        st.session_state.run_clicked = False
    if "urls" not in st.session_state:
        st.session_state.urls = None
    if "external_links_in_results" not in st.session_state:
        st.session_state.external_links_in_results = None

    # User input
    query = st.text_input("Enter your search query:")
    num_results = st.number_input("Enter the number of results to retrieve: (2-100)", min_value=2, max_value=100, value=10)
    lang = st.text_input("Enter the desired search location (e.g., 'us', 'uk', 'fr', 'ir'):")
    exclude_input = st.text_area("Enter websites to exclude (one per line):")
    exclude_websites = [line.strip() for line in exclude_input.split("\n") if line.strip()]

    if st.button("Run"):
        async with aiohttp.ClientSession() as session:  # Create an aiohttp.ClientSession
            # Correctly pass the session to the scrape_google_search_results function
            urls = await scrape_google_search_results(query, num_results, lang, exclude_websites, session)

            if not urls:
                st.error("Failed to fetch search results.")
                return

        max_retries = 2

        async with aiohttp.ClientSession() as session:
            # Check for external links in the search results with retries
            external_links_in_results = await fetch_with_retries(session, urls, max_retries)

            if not external_links_in_results:
                st.error("Failed to fetch external links.")
                return

            # Extract search result domains for filtering
            search_result_domains = [urlparse(website).netloc for website in urls]

            # Check for filtered external links in the search results with retries
            filtered_external_links_in_results = await fetch_with_retries(session, urls, max_retries)

            # Convert the results to a DataFrame
            result_df_data = []
            for idx, url in enumerate(urls, start=1):
                if url in filtered_external_links_in_results:
                    filtered_links = filtered_external_links_in_results[url]
                    if filtered_links:
                        filtered_links_formatted = "\n".join(filtered_links)
                        result_df_data.append((idx, url, filtered_links_formatted))
                    else:
                        result_df_data.append((idx, url, "None"))  # Display "None" for no external links
                else:
                    result_df_data.append((idx, url, "N/A"))  # Display "N/A" if URL not in results

            result_df = pd.DataFrame(result_df_data, columns=["Position", "URL", "Filtered External Links"])

            # Modify the DataFrame to handle multiple external links per row
            modified_result_df_data = []
            for _, row in result_df.iterrows():
                position = row["Position"]
                url = row["URL"]
                filtered_links = row["Filtered External Links"]
                if "\n" in filtered_links:
                    filtered_links = filtered_links.split("\n")
                    for link in filtered_links:
                        modified_result_df_data.append((position, url, link))
                else:
                    modified_result_df_data.append((position, url, filtered_links))
            modified_result_df = pd.DataFrame(modified_result_df_data, columns=["Position", "URL", "Filtered External Links"])

            # Add "#" to repeated URLs in the "URL" column
            modified_result_df["URL"] = modified_result_df["URL"].astype(str) + modified_result_df.duplicated("URL").replace({True: "#", False: ""})

            # Display the DataFrame
            st.write("Filtered external link extractor From Google serp:")
            st.dataframe(modified_result_df)

            # Save the DataFrame to session state
            st.session_state.filtered_result_df = modified_result_df
    # Check if the DataFrame exists in session state and display the export button
    if hasattr(st.session_state, "filtered_result_df"):
        # Add an "Export to Excel" button
        if st.button("Export to Excel"):
            file_name = f"{query}_filtered_search_results.xlsx"
            st.write(f"Exporting to Excel... Please wait.")
            st.session_state.filtered_result_df.to_excel(file_name, index=False)
            st.success(f"Data exported to {file_name}")

if __name__ == "__main__":
    asyncio.run(main_async())