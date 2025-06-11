#My solution uses event-driven simulation. I'll process each action chronologically and maintain separate state for each driver. Each driver tracks their current location and all active orders they're carrying.
def calculateDeliveryTimes(self, actions, travel_time_matrix):
    """
    :type actions: List[Dict[str, Union[str, int]]]
    :type travel_time_matrix: List[List[int]]
    :rtype: List[str]
    """
    #convert location ID to index by subtracting ASCII value of 'A'
    #assuming loc_id is a single uppercase letter from 'A' to 'Z'
    #this function converts a location ID (like 'A', 'B', etc.) to an index (0, 1, etc.)
    #this is used to access the travel_time_matrix
    #for example, 'A' becomes 0, 'B' becomes 1, ..., 'Z' becomes 25
    #this allows us to use the location ID directly as an index in the travel_time_matrix
    #this is a common technique in competitive programming to convert characters to indices


    #initiate driver's state and results list
    #iterate through each action, updating the driver's state and calculating delivery times
       #if a driver is not in drivers, initialize their state with their current location and an empty active orders dictionary
       #if a driver exists, and the privious location is different from their current location, update their location and travel time for all active orders.
    #        #the previous location is extracted from the drivers[driver_id] dictionary 
       #if the order is 'pickup', add the order to the driver's active orders with a time of 0 to initialize it
         #if the order is 'dropoff', check if the order exists in the driver's active orders
         #if yes, get the total time taken for that order
            #remove the order from the driver's active orders --del is to remove the order including its key and value from the dictionary


    def loc_to_index(loc_id):
        return ord(loc_id) - ord('A')
    
    # Track each driver's state: {driver_id: {'location': int, 'orders': {order_num: time}}}
    drivers = {}
    results = []
    #results = {} if dictionary format
    
    for action in actions:
        driver_id = action['driver_id']
        current_loc = loc_to_index(action['loc_id'])
        action_type = action['action_type']
        
        # Initialize driver if new
        if driver_id not in drivers:
            drivers[driver_id] = {'location': current_loc, 'orders': {}}
        
        prev_loc = drivers[driver_id]['location']#'location': int
        orders = drivers[driver_id]['orders'] #orders': {order_num: time}
        
        # Calculate travel time and add to all active orders for this driver
        if prev_loc != current_loc:
            travel_time = travel_time_matrix[prev_loc][current_loc]
            for order_num in orders:
                orders[order_num] += travel_time #all active orders add the travel time 
        
        # Process action
        if action_type == 'pickup':
            orders[action['order_number']] = 0 #'orders': {102: 0}
        elif action_type == 'dropoff':
            order_num = action['order_number']
            total_time = orders[order_num]
            result_msg = f"Order {order_num} is delivered within {total_time} mins"
            print(result_msg)  # Print immediately during processing
            results.append(result_msg)
            #results[order_num] = total_time
            del orders[order_num] #use del orders[order_num] to remove the completed order from the driver's active orders. This prevents delivered orders from continuing to accumulate travel time and keeps the algorithm efficient.
        
        # Update driver location for next iterration
        drivers[driver_id]['location'] = current_loc
    
    # Sort by order number
    results.sort(key=lambda x: int(x.split()[1]))
    #results = dict(sorted(results.items())) 
    #results = dict(sorted(results.items(), key=lambda x: x[1]),reverse= True)
    return results

 
# Time Complexity: O(n * m) where n = number of actions, m = max orders per driver at any time
# Space Complexity: O(d * m) where d = number of drivers, m = max orders per driver