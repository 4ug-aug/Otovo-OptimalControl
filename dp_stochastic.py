def get_series(meter_id, type="prod", start=None, end=None, agg=None):
    """Create Series from meter_id and type of data

    Args:
        meter_id (str): meter-id
        type (str, optional): production or consumption of kwh. Defaults to "prod".
        start (str, optional): timeslot to start series. Defaults to None.
        end (str, optional): timeslot to end series. Defaults to None.
        agg (str, optional): aggregation of data. One of day, week or month. Defaults to None.

    Returns:
        pd.series: series of filtered data
    """

    print("Getting series for meter_id: {}".format(meter_id))

    # if start not none
    if start is not None:
        # Convert to datetime
        start = pd.to_datetime(start)
    
    # if end not none
    if end is not None:
        # Convert to datetime
        end = pd.to_datetime(end)

    if type == "prod":
        df_return = df_prod[df_prod["meter_id"] == meter_id]
        # Drop all columns but timeslot and num_kwh_normalized
        df_return = df_return[['timeslot', 'num_kwh']]
        # Set index to timeslot
        # Filter on start and end
        # Convert timeslot to datetime
        df_return["timeslot"] = pd.to_datetime(df_return["timeslot"], utc=True)
        if start is not None and end is not None:
            print("Filtering on start and end: ", start, end)
            try:
                df_return = df_return[(df_return['timeslot'] >= start) & (df_return['timeslot'] <= end)]
            except Exception as e:
                print(e)
                print("No data for this timeslot, timeslot might be incorrect format or out of range:")
                print("Format and range for timeslot: ", df_return.index[0], " ", df_return.index[-1])
                print("Format for input start: ", start)
                print("Format for input end: ", end)
                pass
        elif start is not None:
            print("Filtering on start: ", start)
            try:
                df_return = df_return[(df_return['timeslot'] >= start)]
            except:
                print("No data for this timeslot, timeslot might be incorrect format or out of range:")
                print("Format and range for timeslot: ", df_return.index[0], " ", df_return.index[-1])
                print("Format for input start: ", start)
                pass
        elif end is not None:
            print("Filtering on end: ", end)
            try:
                df_return = df_return[(df_return['timeslot'] <= end)]
            except:
                print("No data for this timeslot, timeslot might be incorrect format or out of range:")
                print("Format and range for timeslot: ", df_return.index[0], " ", df_return.index[-1])
                print("Format for input end: ", end)
                pass

    elif type == "cons":
        df_return = df_cons[df_cons["meter_id"] == meter_id]
        # Drop all columns but timeslot and num_kwh_normalized
        df_return = df_return[['timeslot', 'num_kwh']]
        # Set index to timeslot
        # Filter on start and end
        # Convert timeslot to datetime
        df_return["timeslot"] = pd.to_datetime(df_return["timeslot"], utc=True)
        if start is not None and end is not None:
            print("Filtering on start and end: ", start, end)
            try:
                df_return = df_return[(df_return['timeslot'] >= start) & (df_return['timeslot'] <= end)]
            except Exception as e:
                print(e)
                print("No data for this timeslot, timeslot might be incorrect format or out of range:")
                print("Format and range for timeslot: ", df_return.index[0], " ", df_return.index[-1])
                print("Format for input start: ", start)
                print("Format for input end: ", end)
                pass
        elif start is not None:
            print("Filtering on start: ", start)
            try:
                df_return = df_return[(df_return['timeslot'] >= start)]
            except:
                print("No data for this timeslot, timeslot might be incorrect format or out of range:")
                print("Format and range for timeslot: ", df_return.index[0], " ", df_return.index[-1])
                print("Format for input start: ", start)
                pass
        elif end is not None:
            print("Filtering on end: ", end)
            try:
                df_return = df_return[(df_return['timeslot'] <= end)]
            except:
                print("No data for this timeslot, timeslot might be incorrect format or out of range:")
                print("Format and range for timeslot: ", df_return.index[0], " ", df_return.index[-1])
                print("Format for input end: ", end)
                pass

    df_return = df_return.set_index("timeslot").sort_index()

    # If agg is not none
    if agg is not None:
        if agg == "day":
            df_return = df_return.resample("D").sum()
        elif agg == "week":
            df_return = df_return.resample("W").sum()
        elif agg == "month":
            df_return = df_return.resample("M").sum()
        else:
            print("Aggregation not supported")
            return None

    # Return series
    return df_return

class DPModel: 
    def __init__(self, N, battery_model, demand, prod, sp): 
        self.N = N
        self.sp = sp  # Spot-price
        self.demand = demand
        self.prod = prod
        self.battery = battery_model

    def f(self, x, u, w, k):
        buy = u[0]
        sell = u[1]
        yield_kwh = u[2]

        update_charge = buy + yield_kwh - sell

        if w == True and k == 0:
            # Reset battery
            self.battery.current_capacity = 0

        self.battery.charge(update_charge)
        return round(self.battery.get_current_capacity(), 0)

    def g(self, x, u, w, k):
        # We removed the upper bound of production on the action space
        # This means we can buy more power than we are producing
        buy = u[0]
        sell = u[1]
        yield_kwh = u[2]
        sell_excess = 0
        h = self.demand.index[k].hour
        day = 358 if self.demand.index[k].day_of_year > 358 else self.demand.index[k].day_of_year
        price = self.sp[str(day)][str(h)]
        saved = 0
        percentage_cut = 0.1

        if x + buy + yield_kwh > self.battery.max_capacity:
            sell_excess = (x + buy + yield_kwh) - self.battery.max_capacity

        if buy == 0 and sell == 0 and yield_kwh < 0:
            saved = yield_kwh*price
        
        if buy == 0 and sell == 0 and yield_kwh > 0:
            saved = -yield_kwh*price
        
        if buy > 0 and sell == 0 and yield_kwh == 0:
            saved = -x*price
        
        cost = buy*price + saved - (sell * price * percentage_cut + sell_excess * price * percentage_cut)

        return cost

    def gN(self, x):
        return 0

    def S(self, k):
        """ Return the set of states S_k """
        return np.arange(0, self.battery.max_capacity+1, 1)

    def A(self, x, k):
        """ Return the set of actions A_k """
        # Buy Upperbound Lowerbound
        yield_ = self.prod.iloc[k].values[0] - self.demand.iloc[k].values[0]

        if yield_ < 0:
            upper_bound_buy = (self.battery.max_capacity - x) + abs(yield_)
            lower_bound_buy = abs(min(0, x + yield_))

        else:
            upper_bound_buy = self.battery.max_capacity - x
            lower_bound_buy = 0
        
        # Sell Upperbound Lowerbound
        # upper_bound_sell = max(x, yield_, x + yield_)
        # lower_bound_sell = 0

        if yield_ < 0:
            upper_bound_sell = 0
            lower_bound_sell = 0

        elif yield_ + x > self.battery.max_capacity:
            upper_bound_sell = x + yield_
            lower_bound_sell = 0
        
        else:
            upper_bound_sell = x + yield_ # + what we buy
            lower_bound_sell = 0

        buy = np.arange(lower_bound_buy, upper_bound_buy+1, 1)
        sell = np.arange(lower_bound_sell, upper_bound_sell+1, 1)
        
        # Return the set of actions A_k as all combinations of buy and sell
        range_ = np.array(list(itertools.product(buy, sell)))
        yield_array = np.array([yield_]*len(range_))
        for i in range_:
            # Add i[0] to i[1]
            if i[1] > 0:
                i[1] += i[0]
        # Add yield_array column to range_
        range_ = np.column_stack((range_, yield_array))
        return range_


def policy_rollout(model, pi, x0):
    """
    Given an environment and policy, should compute one rollout of the policy and compute
    cost of the obtained states and actions. In the deterministic case this corresponds to

    J_pi(x_0)

    in the stochastic case this would be an estimate of the above quantity.

    Note I am passing a policy 'pi' to this function. The policy is in this case itself a function, that is,
    you can write code such as

    > u = pi(x,k)

    in the body below.
    """
    J, x, trajectory, actions = 0, x0, [x0], []
    for k in range(model.N):
        u = pi(x, k)
        J += model.g(x, u , True, k)
        x = model.f(x, u, True, k)
        trajectory.append(x) # update the list of the trajectory
        actions.append(u) # update the list of the actions
    
    J += model.gN(x)
    return J, trajectory, actions

def DP_stochastic(model):
    """
    Implement the stochastic DP algorithm. The implementation follows (Her21, Algorithm 1).
    In case you run into problems, I recommend following the hints in (Her21, Subsection 6.2.1) and focus on the
    case without a noise term; once it works, you can add the w-terms. When you don't loop over noise terms, just specify
    them as w = None in env.f and env.g.
    """
    N = model.N
    J = [{} for _ in range(N + 1)]
    pi = [{} for _ in range(N)]
    J[N] = {x: model.gN(x) for x in model.S(model.N)}
    for k in tqdm(range(N-1, -1, -1)):
        for x in model.S(k): 
            """
            Update pi[k][x] and Jstar[k][x] using the general DP algorithm given in (Her21, Algorithm 1).
            If you implement it using the pseudo-code, I recommend you define Q as a dictionary like the J-function such that
                        
            > Q[u] = Q_u (for all u in model.A(x,k))
            Then you find the u where Q_u is lowest, i.e. 
            > umin = arg_min_u Q[u]
            Then you can use this to update J[k][x] = Q_umin and pi[k][x] = umin.
            """
            w = False
            # print(f"State {x}")
            # print(f"Yield {model.prod.iloc[k].values[0] - model.demand.iloc[k].values[0]}")
            # print(model.A(x, k))
            # exit()

            Qu = {tuple(u): (model.g(x, u, w, k) + J[k + 1][model.f(x, u, w, k)]) for u in model.A(x, k)} 
            umin = min(Qu, key=Qu.get)
            J[k][x] = Qu[umin]
            pi[k][x] = umin


            """
            After the above update it should be the case that:

            J[k][x] = J_k(x)
            pi[k][x] = pi_k(x)
            """
    return J, pi

if __name__ == "__main__":  # Test dp on small graph given in (Her21, Subsection 6.2.1)
    from battery import Battery
    import pandas as pd
    import numpy as np
    import json
    from tqdm import tqdm
    import itertools

    print("Testing the deterministic DP algorithm on the Battery Control problem")

    # N, battery_model, demand, prod, sp
    battery_model = Battery(max_capacity=13.0)

    df_prod = pd.read_csv("data/gridtx-dump-AGGREGATED-CLEANED-THRESHOLD-COVERAGE100-NORMALIZED-PROD.csv")
    df_cons = pd.read_csv("data/gridtx-dump-AGGREGATED-CLEANED-THRESHOLD-COVERAGE100-NORMALIZED-CONS.csv")

    # Load json file hour_lookup_price.json
    with open('data/nordpool_hour_lookup_price_dict.json') as json_file:
        hour_lookup_price = json.load(json_file)

    series_prod = get_series("e882f9a7-f1de-4419-9869-7339be303281", "prod")
    series_cons = get_series("e882f9a7-f1de-4419-9869-7339be303281", "cons")

    # Add possible predictions here
    model = DPModel(series_prod.shape[0], battery_model, series_cons, series_prod, hour_lookup_price)  # Instantiate the small graph with target node 5 
    J, pi = DP_stochastic(model)
    # Print all optimal cost functions J_k(x_k) 

    # for k in range(len(J)):
    #     print(", ".join([f"J_{k}({i}) = {v:.1f}" for i, v in J[k].items()]))
    s = 0.0  # start node
    J, xp, actions = policy_rollout(model, pi=lambda x, k: pi[k][x], x0=s)

    save = True

    if save:
        suffix = "new_meter_id"
        # Save J to a file
        with open(f'data/dp/J_small_graph_{suffix}.txt', 'w') as outfile:
            outfile.write(str(J))
        
        # Save the policy pi to a file
        with open(f'data/dp/pi_small_graph_{suffix}.txt', 'w') as outfile:
            outfile.write(str(pi))
        
        # Save xp and actions to a file
        with open(f'data/dp/xp_small_graph_{suffix}.txt', 'w') as outfile:
            outfile.write(str(xp))
        
        with open(f'data/dp/actions_small_graph_{suffix}.txt', 'w') as outfile:
            outfile.write(str(actions))

    # Create dataframe with columns xp, action, physical_action and cost
    df = pd.DataFrame(columns=['Path', '(Buy, Sell, Yield)', 'Cost'])
    df['Path'] = xp[:-1]
    df['(Buy, Sell, Yield)'] = actions
    df['Cost'] = [model.g(x, u, False, k) for k, (x, u) in enumerate(zip(xp, actions))]

    print(df)

    print(f"Actual cost of rollout was {J} which should obviously be similar to J_0[{s}]")

    timeline = series_prod.index.values

    cost = 0
    for idx, action in enumerate(actions):
        # Get day and hour of action
        timestamp = pd.to_datetime(timeline[idx])
        day = str(358 if timestamp.dayofyear > 358 else timestamp.day_of_year)
        hour = str(timestamp.hour)
        # Get price of hour
        price = hour_lookup_price[day][hour]

        # Get cost of action
        buy = action[0]
        sell = action[1]
        cost += buy * price + sell * price*0.1
    
    print(f"Cost of buyin and selling was {cost}")


