
def calculate_movie_averages(customer_ratings):
    # Step 1: Aggregate ratings by movie_id
    # Initialize a dictionary to hold lists of ratings for each movie_id
    #customer_ratings is a list of dictionaries, each containing 'user_id', 'movie_id', and 'rating'
    #movie_ratings will hold movie_id as keys and lists of ratings as values
    #movie_ratings will be a dictionary where keys are movie_ids and values are lists of ratings
    #we can use a defaultdict from collections to simplify the initialization of lists
    #from collections import defaultdict
    #movie_ratings = defaultdict(list)      
    #average ratings will be stored in a dictionary where keys are movie_ids and values are the average ratings
    movie_ratings = {}
    
    for rating_data in customer_ratings:
        movie_id = rating_data['movie_id']
        rating = rating_data['rating']
        #if rating might be missing inside the dictionary, we can use rating_data.get('rating')
        
         # Skip invalid ratings
        if rating is None or not (0 <= rating <= 5):
            continue
        
        # Initialize list if movie not seen before
        if movie_id not in movie_ratings:
            movie_ratings[movie_id] = []
        
        # Add rating to movie's list
        movie_ratings[movie_id].append(rating)
    
    # Step 2: Calculate averages and round to 1 decimal place
    movie_averages = {}
    for movie_id, ratings in movie_ratings.items():
		    if len(ratings) > 0: #defensive check
		        average = sum(ratings) / len(ratings)
		        movie_averages[movie_id] = round(average, 1)
    
    # Step 3: Sort by movie_id and return
    return dict(sorted(movie_averages.items()))
        #sort by rating desc
    #return dict(sorted(movie_averages.items(), key=lambda x: x[1], reverse=True))


### **Input:**

customer_ratings = [

{'user_id': 'user1', 'movie_id': 'movie1', 'rating': 4},

{'user_id': 'user2', 'movie_id': 'movie2', 'rating': 5},

{'user_id': 'user3', 'movie_id': 'movie1', 'rating': 3}]

---

### **Output:**

{'movie1': 3.5,

'movie2': 5.0}