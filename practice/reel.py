from collections import deque

def process_content_buffer_v1(posts, buffer_size=3):
    # Process a list of posts with a buffer to track engagement and viewing length.
    # Posts with 'test' key set to True are skipped.
    #initialize a deque with a maximum length of buffer_size to store posts,deque is a double-ended queue that allows appending and popping from both ends
    #results list to store messages about engagement and viewing length
    # iterate through each post, skipping those with 'test' key set to True
    # Add posts to the buffer, and when the buffer is full, calculate total engagement and viewing length.when buffer is full, calculate total engagement and viewing length.
    # Return a list of messages summarizing the engagement and viewing length for each full buffer.


    buffer = deque(maxlen=buffer_size)
    results = []

		#variation 1:# Skip test posts completely
    for post in posts:
        if post.get('test', False): #no matter if the value of test is true or false
            continue
    #variation 2:Process post only if test is False or not present, skip if test is explicitly True
    for post in posts:
        test_value = post.get('test')
        if test_value is True:
            continue

        # Add post to buffer (automatically evicts oldest if full)
        buffer.append(post)

        # Process when buffer is full
        #if len(buffer) > 0: #proceed when there's dat
        if len(buffer) == buffer_size:
            total_engagement = sum(p['engagement_ct'] for p in buffer)
            total_time = sum(p['viewing_length'] for p in buffer)
            post_ids = [p['post_id'] for p in buffer]

            message = f"You've got {total_engagement} engagement(s) and spent {total_time}s viewing content. Post ids: {post_ids}"
            results.append(message)
            print(message)

    return results