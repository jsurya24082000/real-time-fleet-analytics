"""
Unit tests for GPS and Delivery simulators.
"""

import pytest
from datetime import datetime
import json

import sys
sys.path.insert(0, '..')

from ingestion.gps_simulator import GPSSimulator, GPSEvent, Vehicle
from ingestion.delivery_simulator import DeliverySimulator, DeliveryEvent, DeliveryStatus


class TestGPSSimulator:
    """Tests for GPS event simulator."""
    
    def test_simulator_initialization(self):
        """Test simulator initializes with correct number of vehicles."""
        simulator = GPSSimulator(num_vehicles=50)
        assert len(simulator.vehicles) == 50
        
    def test_vehicle_creation(self):
        """Test vehicle is created with valid attributes."""
        vehicle = Vehicle("V001", 40.7128, -74.0060)
        assert vehicle.vehicle_id == "V001"
        assert -90 <= vehicle.latitude <= 90
        assert -180 <= vehicle.longitude <= 180
        assert 0 <= vehicle.fuel_level <= 1
        
    def test_gps_event_generation(self):
        """Test GPS event is generated with all required fields."""
        vehicle = Vehicle("V001", 40.7128, -74.0060)
        vehicle.start_engine()
        event = vehicle.generate_event()
        
        assert event.vehicle_id == "V001"
        assert event.event_id is not None
        assert event.timestamp is not None
        assert -90 <= event.latitude <= 90
        assert -180 <= event.longitude <= 180
        assert event.speed_mph >= 0
        assert 0 <= event.heading <= 360
        
    def test_gps_event_json_serialization(self):
        """Test GPS event can be serialized to JSON."""
        vehicle = Vehicle("V001", 40.7128, -74.0060)
        event = vehicle.generate_event()
        
        json_str = event.to_json()
        parsed = json.loads(json_str)
        
        assert parsed['vehicle_id'] == "V001"
        assert 'timestamp' in parsed
        assert 'latitude' in parsed
        
    def test_vehicle_position_update(self):
        """Test vehicle position updates when moving."""
        vehicle = Vehicle("V001", 40.7128, -74.0060)
        vehicle.start_engine()
        
        initial_lat = vehicle.latitude
        initial_lon = vehicle.longitude
        
        vehicle.update_position(10.0)  # 10 seconds
        
        # Position should change if vehicle is moving
        if vehicle.speed_mph > 0:
            assert (vehicle.latitude != initial_lat or 
                    vehicle.longitude != initial_lon)
                    
    def test_fuel_consumption(self):
        """Test fuel decreases when vehicle moves."""
        vehicle = Vehicle("V001", 40.7128, -74.0060)
        vehicle.fuel_level = 1.0
        vehicle.start_engine()
        vehicle.speed_mph = 60
        
        initial_fuel = vehicle.fuel_level
        vehicle.update_position(3600)  # 1 hour at 60 mph = 60 miles
        
        assert vehicle.fuel_level < initial_fuel
        
    def test_batch_generation(self):
        """Test batch event generation."""
        simulator = GPSSimulator(num_vehicles=100)
        events = simulator.generate_batch(batch_size=50)
        
        assert len(events) == 50
        assert all(isinstance(e, GPSEvent) for e in events)


class TestDeliverySimulator:
    """Tests for delivery event simulator."""
    
    def test_simulator_initialization(self):
        """Test delivery simulator initializes correctly."""
        simulator = DeliverySimulator(orders_per_hour=500, num_vehicles=100)
        assert simulator.orders_per_hour == 500
        assert len(simulator.vehicle_ids) == 100
        
    def test_delivery_event_generation(self):
        """Test delivery event has all required fields."""
        simulator = DeliverySimulator()
        events = simulator.generate_batch(batch_size=10)
        
        assert len(events) > 0
        event = events[0]
        
        assert event.delivery_id is not None
        assert event.order_id is not None
        assert event.event_type is not None
        assert event.timestamp is not None
        assert event.customer_id is not None
        
    def test_delivery_event_json_serialization(self):
        """Test delivery event can be serialized to JSON."""
        simulator = DeliverySimulator()
        events = simulator.generate_batch(batch_size=1)
        event = events[0]
        
        json_str = event.to_json()
        parsed = json.loads(json_str)
        
        assert 'delivery_id' in parsed
        assert 'order_id' in parsed
        assert 'event_type' in parsed
        
    def test_order_status_progression(self):
        """Test orders progress through status states."""
        simulator = DeliverySimulator()
        
        # Generate initial orders
        events = simulator.generate_batch(batch_size=20)
        
        # Should have ORDER_PLACED events
        order_placed = [e for e in events if e.event_type == 'ORDER_PLACED']
        assert len(order_placed) > 0
        
    def test_delivery_status_values(self):
        """Test all delivery status values are valid."""
        valid_statuses = {s.value for s in DeliveryStatus}
        
        simulator = DeliverySimulator()
        
        # Generate many events to cover different statuses
        for _ in range(10):
            events = simulator.generate_batch(batch_size=20)
            for event in events:
                assert event.event_type in valid_statuses


class TestIntegration:
    """Integration tests for simulators."""
    
    def test_gps_and_delivery_vehicle_ids_match(self):
        """Test that vehicle IDs are consistent across simulators."""
        gps_sim = GPSSimulator(num_vehicles=100)
        delivery_sim = DeliverySimulator(num_vehicles=100)
        
        gps_vehicle_ids = {v.vehicle_id for v in gps_sim.vehicles}
        delivery_vehicle_ids = set(delivery_sim.vehicle_ids)
        
        # Both should have same format
        assert all(v.startswith('V') for v in gps_vehicle_ids)
        assert all(v.startswith('V') for v in delivery_vehicle_ids)
        
    def test_high_volume_generation(self):
        """Test simulators can handle high volume generation."""
        gps_sim = GPSSimulator(num_vehicles=500)
        delivery_sim = DeliverySimulator(orders_per_hour=1000)
        
        # Generate 1000 GPS events
        gps_events = []
        for _ in range(10):
            gps_events.extend(gps_sim.generate_batch(100))
        assert len(gps_events) == 1000
        
        # Generate 500 delivery events
        delivery_events = []
        for _ in range(10):
            delivery_events.extend(delivery_sim.generate_batch(50))
        assert len(delivery_events) == 500


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
