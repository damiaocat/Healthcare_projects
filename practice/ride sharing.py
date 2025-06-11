# Question 1: Write a function that takes a list of bookings and vehicle capacity, then returns True if the vehicle can handle all bookings without exceeding capacity, False otherwise.
def can_handle_bookings(bookings, capacity):
    #create a list to track the number of passengers at each time point
    #saving tuple of (time, change in passengers) into the list
    #sort the list by time
    #sum the changes in passengers at each time point
    #if the number of passengers exceeds capacity at any point, return False
    #else, return True
    #base case should be if bookings is empty, return True
    if not bookings:
        return True
    if capacity <= 0:
        return False
    
    events = []
    for booking in bookings:
        start = booking['start']
        end = booking['end']
        num = booking['num']
        if num > capacity:
            return False
        events.append((start, num))
        events.append((end,-num))
    
    events = sorted(events, key=lambda x: (x[0],x[1]>0)) #we use x[1]>0 to ensure that if a booking ends and another starts at the same time, the end is processed first. Since false is less than true, this will ensure that the end of a booking is processed before the start of a new booking at the same time.
    current = 0
    for time, change in events:
        current += change
        if current > capacity:
            return False
    
    return True

# Test case
bookings = [{'start': 8, 'end': 10, 'num': 3}, {'start': 9, 'end': 11, 'num': 2}]
capacity = 6
print(can_handle_bookings(bookings, capacity))  # Should return True