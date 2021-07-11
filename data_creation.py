# Dependencies
import wikipedia

# Function for importaning the pages
def import_wikipedia(list_searches):
    data_searches, count = [], 0
    for search in list_searches:
        try:
            p = wikipedia.page(search)
            data_searches.append(p.content)
            count += 1
        except:
            pass
    return data_searches, count