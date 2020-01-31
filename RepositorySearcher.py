import urllib
import numpy as np
import time
import pandas as pd
import re
from tqdm import tqdm
import requests
from fake_useragent import UserAgent
from bs4 import BeautifulSoup


class RepositoriesSearcher:

    def __init__(self, request_language=None, folders_limit=None, source_code_files_limit=None, verbose=False):
        self.request_language = request_language
        self.folders_limit = folders_limit
        self.source_code_files_limit = source_code_files_limit
        self.verbose = verbose
        self.git_url = 'https://github.com'
        self.git_raw_url = 'https://raw.githubusercontent.com'

    def create_dataset(self, request_queues, page_limit=100, to_csv=None):
        if type(request_queues) is str:
            request_queues = [request_queues]
        data_arr = np.empty((0, 5))
        for request_queue in request_queues:
            # Collect all links from 'page_limit' number of pages
            all_repos_links = self.collect_repositories(request_queue, page_limit)
            # Remove links duplicates from previous iterations
            all_repos_links = np.setdiff1d(all_repos_links, data_arr[:, 0])
            # Collect the links of repositories contents
            rep_contents = np.array(self.collect_repositories_contents(all_repos_links))
            # Scrap the contents of collected links
            parsed_repos = np.array(self.parse_folders(rep_contents[:, 1]))
            collected_data = np.column_stack((rep_contents[:, 0], parsed_repos, rep_contents[:, 2:4]))
            if self.verbose: print()
            data_arr = np.append(data_arr, collected_data, axis=0)
            if to_csv:
                columns = ['Link', 'SourceCode', 'Description', 'SourceCodeFilesNum', 'FoldersNum']
                parsed_repos_df = pd.DataFrame(data=data_arr, columns=columns)
                parsed_repos_df.to_csv(to_csv, index=False)
        if to_csv is None:
            columns = ['Link', 'SourceCode', 'Description', 'SourceCodeFilesNum', 'FoldersNum']
            parsed_repos_df = pd.DataFrame(data=data_arr, columns=columns)
        return parsed_repos_df

    def create_request_url(self, request_queue, page=1):
        git_search_url = f'{self.git_url}/search?'
        language_url = self.request_language
        if language_url:
            language_url = f'l={self.request_language}'
        page_url = f'p={page}'
        queue_url = f'q={urllib.parse.quote_plus(request_queue)}'
        type_url = 'type=Repositories'
        return git_search_url + '&'.join([language_url, page_url, queue_url, type_url])

    def parse_repositories_from_page(self, soup_page):
        repos = []
        for link in soup_page.find_all('a', {'class': 'v-align-middle'}):
            repos.append(link.get('href'))
        return repos

    def collect_repositories(self, request_queue, page_limit=100):
        page, total_pages = 1, 1
        all_repos_links = []
        while (page <= total_pages) and (page <= page_limit):
            url = self.create_request_url(request_queue, page)
            response = self.resolve_redirects(url, 120)
            soup = BeautifulSoup(response.content, 'html.parser')
            all_repos_links = all_repos_links + self.parse_repositories_from_page(soup)
            if page == 1:
                total_repos_class = soup.find('span', {'class': 'ml-1 mt-1 js-codesearch-count Counter Counter--gray',
                                                       'data-search-type': 'Repositories'})
                if total_repos_class and self.verbose:
                    print(
                        'Found ' + str(total_repos_class.text) + ' repositories for the queue: "' + request_queue + '"')
                total_pages_class = soup.find('em', {'class': 'current'})
                if total_pages_class:
                    total_pages = int(total_pages_class['data-total-pages'])
            if self.verbose: print(f"{page} page. {len(all_repos_links)} links")
            page += 1
            time.sleep(6 * np.random.rand())
        if self.verbose: print(f'Parsed {page - 1} pages')
        return all_repos_links

    def collect_repositories_contents(self, href_lst):
        if self.verbose: print('Collecting...')
        data_arr = []
        for href in tqdm(href_lst):
            link = self.git_url + href
            response = self.resolve_redirects(link, 120)
            rep_soup = BeautifulSoup(response.content, 'html.parser')

            if rep_soup.find_all('a', {'title': re.compile('\.md$')}):
                collected_links, folders_num, source_code_files_num = self._get_dfs_repository_links(href)
                if collected_links is not None and (source_code_files_num > 0):
                    data_arr.append([href, collected_links, source_code_files_num, folders_num])
        if self.verbose:
            disposed_num = len(href_lst) - len(data_arr)
            if disposed_num > 0: print(f'Disposed of {disposed_num} repositories due to the limitations')
        return data_arr

    def parse_folders(self, folders_lst):
        if self.verbose: print('Parsing...')
        parsed_folders = []
        for href_lst in tqdm(folders_lst):
            source_code, description = '', ''
            for href in href_lst:
                raw_url = self.git_raw_url + href
                raw_response = self.resolve_redirects(raw_url, 120)
                if re.search('readme\.md$', href.lower()):
                    try:
                        description = description + raw_response.text + '\n'
                    except UnicodeDecodeError:
                        description = description + 'Unreadable description\n'
                else:
                    try:
                        source_code = source_code + raw_response.text + '\n'
                    except UnicodeDecodeError:
                        source_code = source_code + 'Unreadable Source Code\n'
            parsed_folders.append([source_code, description])
        return parsed_folders

    def _get_dfs_repository_links(self, href, folders_num=None, source_code_files_num=None):
        if (folders_num is None) or (source_code_files_num is None):
            source_code_files_num, folders_num = 0, 0
        files_num = source_code_files_num
        folders_num += 1
        if folders_num > self.folders_limit:
            return None, folders_num, source_code_files_num
        link = self.git_url + href
        response = self.resolve_redirects(link, sleep_time=120)
        rep_soup = BeautifulSoup(response.content, 'html.parser')
        collected_links = []
        for td in rep_soup.find_all('td', {'class': 'content'}):
            # Find links
            link = td.find('a')
            if link:
                link_title = link.get('title')
                link_href = link.get('href')
                # If the link leads to an object
                if link_href.find('/blob/') > -1:
                    if link_title.lower() == "readme.md":
                        collected_links.append(link_href.replace('/blob/', '/'))
                    elif re.search('\.(?:py|java|c|cpp|h)$', link_title):
                        collected_links.append(link_href.replace('/blob/', '/'))
                        source_code_files_num += 1
                    if source_code_files_num > self.source_code_files_limit:
                        collected_links = None
                        break
                # If the link leads to a folder
                elif link_href.find('/tree/') > -1:
                    if (not re.search('/v?env$', link_href)) and (link_title != '.github'):
                        folder_links, folders_num, source_code_files_num = self._get_dfs_repository_links(link_href,
                                                                                                          folders_num,
                                                                                                          source_code_files_num)
                        if folder_links is None:
                            collected_links = None
                            break
                        collected_links = collected_links + folder_links
        if source_code_files_num - files_num == 0:
            folders_num -= 1
        return collected_links, folders_num, source_code_files_num

    def resolve_redirects(self, url, sleep_time=120):
        response = requests.get(url, headers={'User-Agent': UserAgent().chrome})
        if response.status_code == 429:
            if self.verbose: print(f"Status_code {response.status_code} waiting {sleep_time} seconds")
            time.sleep(sleep_time)
            response = self.resolve_redirects(url, sleep_time + 60 + (np.random.rand() - 0.5) * 15)
        elif response.status_code == 404:
            if self.verbose: print(f"Status_code {response.status_code}, url: {url} not found ")
        return response