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

        needed = (self.demand.iloc[k] - self.prod.iloc[k]).values[0]
        rest_discharge = 0

        if u < needed:
            rest_discharge = -(needed - u)
        elif u == needed:
            rest_discharge = -needed

        # If true we simulate a battery
        if w == True:
            temp_battery = Battery(current_capacity=x)
            temp_battery.charge(u)
            temp_battery.charge(rest_discharge)

            return round(temp_battery.get_current_capacity(), 0)


        # If we buy less power than we need, we discharge the rest from the battery
        self.battery.charge(u)
        self.battery.charge(rest_discharge)

        return round(self.battery.get_current_capacity(), 0)

    def g(self, x, u, w, k):
        # We removed the upper bound of production on the action space
        # This means we can buy more power than we are producing
        prod = self.prod.iloc[k].values[0]
        demand = self.demand.iloc[k].values[0]
        bought = 0

        # If our action is greater than the production, we buy more power
        if u > prod:
            bought = u - prod
            u = prod

        rt = (self.f(x, u, True, k) - x)
        potential_saving = demand - prod + bought
        nt = (demand - prod - bought) + rt

        h = self.demand.index[k].hour

        #  + bought*self.sp[str(h)]

        if nt > 0:
            # We buy power
            return nt*self.sp[str(h)]
        elif nt < 0:
            # We sell power
            return potential_saving*self.sp[str(h)] - nt*self.sp[str(h)]*0.25
        return bought*self.sp[str(h)]

    def gN(self, x):
        return 0

    def S(self, k):
        """ Return the set of states S_k """
        return np.arange(0, self.battery.max_capacity+1, 1)

    def A(self, x, k):
        """ Return the set of actions A_k """
        # We can only discharge the battery down to 0 with a max discharge rate of 7
        production = self.prod.iloc[k].values[0]
        demand = self.demand.iloc[k].values[0]
        # We always have to buy atleast our needed power
        # den her skal tunes! - ligenu kan vi købe mindre end hvad vi har brug for
        needed =  (production + x) - demand if (production + x) >= demand and (production + x) - demand > 0 else production - demand

        max_discharge = 7
        # lower_bound = -min(-demand, max_discharge, x)
        lower_bound = -min(needed, max_discharge, x)
        # The upper bound is defined based on the max amount we are allowed to discharge and the amount we can charge
        upper_bound = min(max_discharge, self.battery.max_capacity - x)
        return np.arange(lower_bound, upper_bound+1, 1)


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
    J, x, trajectory, actions, physical_action = 0, x0, [x0], [], []
    for k in range(model.N):
        u = pi(x, k)
        J += model.g(x, u , True, k)
        x = model.f(x, u, True, k)
        trajectory.append(x) # update the list of the trajectory
        actions.append(u) # update the list of the actions

        if u > 0 and u < model.prod.iloc[k].values[0]:
            physical_action.append("Charge")
        elif u > 0 and u >= model.prod.iloc[k].values[0]:
            physical_action.append("Buy")
        elif u < 0 and u > -model.demand.iloc[k].values[0]:
            physical_action.append("Sell")
        elif u < 0 and u <= -model.demand.iloc[k].values[0]:
            physical_action.append("Use")
        elif u == 0 and model.demand.iloc[k].values[0] - model.prod.iloc[k].values[0] > 0:
            physical_action.append("Use")
        elif model.prod.iloc[k].values[0] + model.battery.max_capacity > model.battery.max_capacity:
            physical_action.append("Sell")
        else:
            physical_action.append("No action")
    
    J += model.gN(x)
    return J, trajectory, actions, physical_action

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
            w = None

            Qu = {u: (model.g(x, u, w, k) + J[k + 1][model.f(x, u, w, k)]) for u in model.A(x, k)} 
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

    print("Testing the deterministic DP algorithm on the Battery Control problem")

    # N, battery_model, demand, prod, sp
    battery_model = Battery(max_capacity=13.0)

    df_prod = pd.read_csv("data/gridtx-dump-AGGREGATED-CLEANED-THRESHOLD-COVERAGE100-NORMALIZED-PROD.csv")
    df_cons = pd.read_csv("data/gridtx-dump-AGGREGATED-CLEANED-THRESHOLD-COVERAGE100-NORMALIZED-CONS.csv")

    # Load json file hour_lookup_price.json
    with open('data/hour_lookup_price_dict.json') as json_file:
        hour_lookup_price = json.load(json_file)

    peak_hours = [8, 9, 10, 11, 16, 17, 18, 19]

    # Update hour_lookup_price by multiplying peak_hours by 8
    for key, value in hour_lookup_price.items():
        if int(key) in peak_hours:
            hour_lookup_price[key] = value * 1.8

    series_prod = get_series("28ba7f57-6e83-4341-8078-232c1639e4e3", "prod", start="2016-09-04 22:00:00+00:00", end = "2019-05-14 21:00:00+00:00")[:25]
    series_cons = get_series("28ba7f57-6e83-4341-8078-232c1639e4e3", "cons", start="2016-09-04 22:00:00+00:00", end = "2019-05-14 21:00:00+00:00")[:25]

    # Add possible predictions here
    model = DPModel(series_prod.shape[0], battery_model, series_cons, series_prod, hour_lookup_price)  # Instantiate the small graph with target node 5 
    J, pi = DP_stochastic(model)
    # Print all optimal cost functions J_k(x_k) 

    # for k in range(len(J)):
    #     print(", ".join([f"J_{k}({i}) = {v:.1f}" for i, v in J[k].items()]))
    s = 0.0  # start node
    J,xp, actions, physical_actions = policy_rollout(model, pi=lambda x, k: pi[k][x], x0=s)

    save = False    

    if save:
        suffix = "mulitplied_peak_buy_allowed"
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
    df = pd.DataFrame(columns=['Path', 'Action (u)', 'Action (Physical)', 'Cost', 'Prod', 'Cons', "Yield"])
    df['Path'] = xp[:-1]
    df['Action (u)'] = actions
    df['Action (Physical)'] = physical_actions
    df['Cost'] = [model.g(x, u, False, k) for k, (x, u) in enumerate(zip(xp, actions))]
    df['Prod'] = list(series_prod["num_kwh"])
    df['Cons'] = list(series_cons["num_kwh"])
    df['Yield'] = df['Prod'] - df['Cons']

    print(df)

    # print(f"Path was", xp)
    # print()
    # print(f"Actions taken", actions)
    # print(f"Physical actions taken", physical_actions)
    # print()
    # print(f"Cost of actions taken", [model.g(x, u, 0, k) for k, (x, u) in enumerate(zip(xp, actions))])
    # print()
    # print(series_prod - series_cons)
    # print()
    print(f"Actual cost of rollout was {J} which should obviously be similar to J_0[{s}]")
    # Remember to check optimal path agrees with the the (self-evident) answer from the figure.


