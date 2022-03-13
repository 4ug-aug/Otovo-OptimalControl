def circle(center, radius):
    """Parameterise a circle with a given radius

    Args:
        radius (float): Radius of the circle
        center (tuple): Tuple containing the center coordinates for the circle

    Return:
        tuple containing circle coordinates (x,y)
    """
    import numpy as np

    theta = np.linspace( 0, 2 * np.pi, 150)

    x = center[0] + radius * np.cos(theta)
    y = center[1]+ radius * np.sin(theta)

    return x,y


if __name__ == "__main__":
    x1,y1 = circle((1,2), 1)
    x2,y2 = circle((-0.5,-1), 2)

    import matplotlib.pyplot as plt 
    plt.plot(x1,y1)
    plt.plot(x2,y2)
    plt.show();