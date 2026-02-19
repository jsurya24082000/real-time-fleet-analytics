"""
GPS Event Simulator
Generates realistic GPS tracking events for fleet vehicles.
"""

import json
import random
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
import math


@dataclass
class GPSEvent:
    """GPS tracking event from a vehicle."""
    event_id: str
    vehicle_id: str
    timestamp: str
    latitude: float
    longitude: float
    speed_mph: float
    heading: int  # 0-360 degrees
    fuel_level: float  # 0.0 to 1.0
    engine_status: str  # ON, OFF, IDLE
    odometer_miles: float
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())


class Vehicle:
    """Simulated vehicle with state tracking."""
    
    def __init__(self, vehicle_id: str, start_lat: float, start_lon: float):
        self.vehicle_id = vehicle_id
        self.latitude = start_lat
        self.longitude = start_lon
        self.speed_mph = 0.0
        self.heading = random.randint(0, 360)
        self.fuel_level = random.uniform(0.5, 1.0)
        self.engine_status = "OFF"
        self.odometer_miles = random.uniform(10000, 100000)
        self.is_on_delivery = False
        self.destination = None
        
    def update_position(self, delta_seconds: float):
        """Update vehicle position based on speed and heading."""
        if self.engine_status == "OFF":
            return
            
        # Convert speed to distance
        distance_miles = (self.speed_mph / 3600) * delta_seconds
        
        # Convert heading to radians
        heading_rad = math.radians(self.heading)
        
        # Update position (approximate, good enough for simulation)
        # 1 degree latitude ≈ 69 miles
        # 1 degree longitude ≈ 69 * cos(latitude) miles
        self.latitude += (distance_miles * math.cos(heading_rad)) / 69
        self.longitude += (distance_miles * math.sin(heading_rad)) / (69 * math.cos(math.radians(self.latitude)))
        
        # Update odometer
        self.odometer_miles += distance_miles
        
        # Consume fuel (roughly 0.03 gallons per mile, 20 gallon tank)
        self.fuel_level -= (distance_miles * 0.03) / 20
        self.fuel_level = max(0.0, self.fuel_level)
        
        # Random heading adjustments (simulate turns)
        if random.random() < 0.1:
            self.heading = (self.heading + random.randint(-30, 30)) % 360
            
    def start_engine(self):
        self.engine_status = "ON"
        self.speed_mph = random.uniform(25, 45)
        
    def stop_engine(self):
        self.engine_status = "OFF"
        self.speed_mph = 0
        
    def idle(self):
        self.engine_status = "IDLE"
        self.speed_mph = 0
        
    def generate_event(self) -> GPSEvent:
        """Generate a GPS event for current state."""
        return GPSEvent(
            event_id=str(uuid.uuid4()),
            vehicle_id=self.vehicle_id,
            timestamp=datetime.utcnow().isoformat() + "Z",
            latitude=round(self.latitude, 6),
            longitude=round(self.longitude, 6),
            speed_mph=round(self.speed_mph, 1),
            heading=self.heading,
            fuel_level=round(self.fuel_level, 3),
            engine_status=self.engine_status,
            odometer_miles=round(self.odometer_miles, 1)
        )


class GPSSimulator:
    """
    Simulates GPS events for a fleet of vehicles.
    
    Usage:
        simulator = GPSSimulator(num_vehicles=100)
        for event in simulator.generate_events(duration_seconds=3600):
            print(event.to_json())
    """
    
    # Major US city coordinates for realistic simulation
    CITY_CENTERS = [
        (40.7128, -74.0060),   # New York
        (34.0522, -118.2437),  # Los Angeles
        (41.8781, -87.6298),   # Chicago
        (29.7604, -95.3698),   # Houston
        (33.4484, -112.0740),  # Phoenix
        (39.7392, -104.9903),  # Denver
        (47.6062, -122.3321),  # Seattle
        (25.7617, -80.1918),   # Miami
        (33.7490, -84.3880),   # Atlanta
        (42.3601, -71.0589),   # Boston
    ]
    
    def __init__(self, num_vehicles: int = 100, region: Optional[str] = None):
        """
        Initialize the GPS simulator.
        
        Args:
            num_vehicles: Number of vehicles to simulate
            region: Optional region name (not used, for future expansion)
        """
        self.num_vehicles = num_vehicles
        self.vehicles: List[Vehicle] = []
        self._initialize_fleet()
        
    def _initialize_fleet(self):
        """Create the simulated vehicle fleet."""
        for i in range(self.num_vehicles):
            vehicle_id = f"V{str(i+1).zfill(4)}"
            
            # Assign vehicle to a random city
            city_lat, city_lon = random.choice(self.CITY_CENTERS)
            
            # Add some randomness to starting position (within ~10 miles)
            start_lat = city_lat + random.uniform(-0.15, 0.15)
            start_lon = city_lon + random.uniform(-0.15, 0.15)
            
            vehicle = Vehicle(vehicle_id, start_lat, start_lon)
            
            # Randomly start some vehicles
            if random.random() < 0.6:
                vehicle.start_engine()
                
            self.vehicles.append(vehicle)
            
    def _simulate_vehicle_behavior(self, vehicle: Vehicle):
        """Simulate realistic vehicle behavior changes."""
        rand = random.random()
        
        if vehicle.engine_status == "OFF":
            # 5% chance to start engine
            if rand < 0.05:
                vehicle.start_engine()
        elif vehicle.engine_status == "ON":
            # 2% chance to stop, 3% chance to idle
            if rand < 0.02:
                vehicle.stop_engine()
            elif rand < 0.05:
                vehicle.idle()
            else:
                # Vary speed
                vehicle.speed_mph = max(0, min(75, vehicle.speed_mph + random.uniform(-5, 5)))
        else:  # IDLE
            # 10% chance to start moving, 5% chance to stop
            if rand < 0.10:
                vehicle.start_engine()
            elif rand < 0.15:
                vehicle.stop_engine()
                
        # Refuel if low
        if vehicle.fuel_level < 0.1 and vehicle.engine_status != "OFF":
            vehicle.stop_engine()
            vehicle.fuel_level = random.uniform(0.8, 1.0)
            
    def generate_events(self, duration_seconds: int = 3600, interval_seconds: float = 1.0):
        """
        Generate GPS events for the specified duration.
        
        Args:
            duration_seconds: How long to run the simulation
            interval_seconds: Time between event batches
            
        Yields:
            GPSEvent objects
        """
        start_time = time.time()
        last_update = start_time
        
        while (time.time() - start_time) < duration_seconds:
            current_time = time.time()
            delta = current_time - last_update
            
            for vehicle in self.vehicles:
                # Update vehicle state
                self._simulate_vehicle_behavior(vehicle)
                vehicle.update_position(delta)
                
                # Generate event (not all vehicles report every interval)
                if random.random() < 0.8:  # 80% reporting rate
                    yield vehicle.generate_event()
                    
            last_update = current_time
            time.sleep(interval_seconds)
            
    def generate_batch(self, batch_size: int = 100) -> List[GPSEvent]:
        """Generate a batch of GPS events."""
        events = []
        for vehicle in random.sample(self.vehicles, min(batch_size, len(self.vehicles))):
            self._simulate_vehicle_behavior(vehicle)
            vehicle.update_position(1.0)
            events.append(vehicle.generate_event())
        return events


def main():
    """Run the GPS simulator in standalone mode."""
    import argparse
    
    parser = argparse.ArgumentParser(description="GPS Event Simulator")
    parser.add_argument("--vehicles", type=int, default=100, help="Number of vehicles")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")
    parser.add_argument("--interval", type=float, default=1.0, help="Event interval")
    parser.add_argument("--output", type=str, default=None, help="Output file (JSON lines)")
    
    args = parser.parse_args()
    
    simulator = GPSSimulator(num_vehicles=args.vehicles)
    
    print(f"Starting GPS simulation with {args.vehicles} vehicles for {args.duration} seconds...")
    
    event_count = 0
    output_file = open(args.output, "w") if args.output else None
    
    try:
        for event in simulator.generate_events(args.duration, args.interval):
            event_json = event.to_json()
            
            if output_file:
                output_file.write(event_json + "\n")
            else:
                print(event_json)
                
            event_count += 1
            
    except KeyboardInterrupt:
        print("\nSimulation stopped by user.")
    finally:
        if output_file:
            output_file.close()
            
    print(f"\nGenerated {event_count} GPS events.")


if __name__ == "__main__":
    main()
