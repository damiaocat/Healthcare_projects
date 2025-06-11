def compute_delivery_times(logs, travel_time_matrix):
    #initate drivers dictionary to track each driver's state and results list to store results of delivered orders
    #iterate through each log entry, updating the driver's state and calculating delivery times
    #if a driver not in drivers, initialize their state with their current location and an empty active orders dictionary
    #if a driver already exists, update their location and travel time for all active orders. 
       #the previous location is extracted from the drivers[driver_id] dictionary
       #if the previous location is different from the current location, calculate the travel time 
       # and update all active orders accordingly
       #update the driver's location to the current location
    #if the action is 'pick up', add the order to the driver's active orders with a time of 0 to initialize it
    #if the action is 'drop off'
      # check if the order exists in the driver's active orders
      # if yes, get the total time taken for that order
        # remove the order from the driver's active orders --del is to remove the order including its key and value from the dictionary
        # append the result to the results list in the format "Order {order_no} is delivered within {total_time} mins" 

  
    drivers = {} # Dictionary to track each driver's state
    # drivers = {driver_id: {'location': loc, 'active_orders': {order_no: time}}}
    # drivers = {1: {'location': 0, 'active_orders': {102: 0}}}
    results = [] # List to store results of delivered orders
    for log in logs: 
        driver_id = log['driver']
        loc = log['location_id']
        action = log['action_type']
        order_no = log['order_no']

        if driver_id not in drivers:
            drivers[driver_id] = {'location': loc, 'active_orders': {}}
        else:
            driver = drivers[driver_id]
            prev_loc = driver['location']
            if prev_loc != loc:
                travel_time = travel_time_matrix[prev_loc][loc]
                for order in driver['active_orders']:
                    driver['active_orders'][order] += travel_time
                driver['location'] = loc

        if action == 'pick up':
            drivers[driver_id]['active_orders'][order_no] = 0
        elif action == 'drop off':
            if order_no in drivers[driver_id]['active_orders']:
                total_time = drivers[driver_id]['active_orders'][order_no]
                del drivers[driver_id]['active_orders'][order_no]
                results.append(f"Order {order_no} is delivered within {total_time} mins")
    return results
