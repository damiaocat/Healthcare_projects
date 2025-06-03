def process_content_buffer_v3(posts, buffer_size=3):
    buffer = deque(maxlen=buffer_size)
    results = []

    for post in posts:
        # Add ALL posts to buffer (including test posts)
        buffer.append(post)

        # Process when buffer is full
        if len(buffer) == buffer_size:
            # # Calculate metrics excluding test posts
            # non_test_posts = []
            # for p in buffer:
            #     if not p.get('test', False):
            #         non_test_posts.append(p)

            if non_test_posts:  # Only print if there are non-test posts
                total_engagement = sum(p['engagement_ct'] for p in non_test_posts)
                total_time = sum(p['viewing_length'] for p in non_test_posts)
                post_ids = [p['post_id'] for p in non_test_posts]

                message = f"You've got {total_engagement} engagement(s) and spent {total_time}s viewing content. Post ids: {post_ids}"
                results.append(message)
                print(message)

    return results