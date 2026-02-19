"""
Delivery Event Simulator
Generates realistic delivery and order events for logistics tracking.
"""

import json
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from enum import Enum


class DeliveryStatus(Enum):
    ORDER_PLACED = "ORDER_PLACED"
    ORDER_CONFIRMED = "ORDER_CONFIRMED"
    PICKED_UP = "PICKED_UP"
    IN_TRANSIT = "IN_TRANSIT"
    OUT_FOR_DELIVERY = "OUT_FOR_DELIVERY"
    DELIVERED = "DELIVERED"
    FAILED_ATTEMPT = "FAILED_ATTEMPT"
    RETURNED = "RETURNED"


@dataclass
class DeliveryEvent:
    """Delivery tracking event."""
    event_id: str
    delivery_id: str
    order_id: str
    vehicle_id: str
    driver_id: str
    event_type: str
    timestamp: str
    latitude: float
    longitude: float
    customer_id: str
    package_weight_lbs: float
    estimated_delivery_time: str
    actual_delivery_time: Optional[str]
    delay_minutes: int
    notes: str
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass
class Order:
    """Represents a customer order."""
    order_id: str
    customer_id: str
    pickup_lat: float
    pickup_lon: float
    delivery_lat: float
    delivery_lon: float
    package_weight_lbs: float
    created_at: datetime
    estimated_delivery: datetime
    status: DeliveryStatus
    vehicle_id: Optional[str] = None
    driver_id: Optional[str] = None
    delivery_id: Optional[str] = None


class DeliverySimulator:
    """
    Simulates delivery events for a logistics operation.
    
    Usage:
        simulator = DeliverySimulator(orders_per_hour=500)
        for event in simulator.generate_events(duration_seconds=3600):
            print(event.to_json())
    """
    
    # Sample customer IDs
    CUSTOMER_POOL_SIZE = 10000
    
    # Driver pool
    DRIVER_POOL_SIZE = 200
    
    # Delivery notes templates
    DELIVERY_NOTES = [
        "Left at front door",
        "Handed to customer",
        "Left with neighbor",
        "Left in mailroom",
        "Signature obtained",
        "Left at back door per customer request",
        "Delivered to reception",
        "",
    ]
    
    FAILED_NOTES = [
        "Customer not available",
        "Address not found",
        "Access denied to building",
        "Package too large for mailbox",
        "Customer refused delivery",
        "Business closed",
    ]
    
    # City coordinates for realistic simulation
    CITY_CENTERS = [
        (40.7128, -74.0060),   # New York
        (34.0522, -118.2437),  # Los Angeles
        (41.8781, -87.6298),   # Chicago
        (29.7604, -95.3698),   # Houston
        (33.4484, -112.0740),  # Phoenix
    ]
    
    def __init__(self, orders_per_hour: int = 500, num_vehicles: int = 100):
        """
        Initialize the delivery simulator.
        
        Args:
            orders_per_hour: Average number of new orders per hour
            num_vehicles: Number of delivery vehicles
        """
        self.orders_per_hour = orders_per_hour
        self.num_vehicles = num_vehicles
        self.active_orders: Dict[str, Order] = {}
        self.completed_orders: List[str] = []
        self.vehicle_ids = [f"V{str(i+1).zfill(4)}" for i in range(num_vehicles)]
        self.driver_ids = [f"D{str(i+1).zfill(4)}" for i in range(self.DRIVER_POOL_SIZE)]
        
    def _generate_order(self) -> Order:
        """Generate a new customer order."""
        city_lat, city_lon = random.choice(self.CITY_CENTERS)
        
        # Pickup location (warehouse/store)
        pickup_lat = city_lat + random.uniform(-0.05, 0.05)
        pickup_lon = city_lon + random.uniform(-0.05, 0.05)
        
        # Delivery location (customer)
        delivery_lat = city_lat + random.uniform(-0.2, 0.2)
        delivery_lon = city_lon + random.uniform(-0.2, 0.2)
        
        now = datetime.utcnow()
        
        return Order(
            order_id=f"ORD-{uuid.uuid4().hex[:8].upper()}",
            customer_id=f"CUST-{random.randint(1, self.CUSTOMER_POOL_SIZE):05d}",
            pickup_lat=pickup_lat,
            pickup_lon=pickup_lon,
            delivery_lat=delivery_lat,
            delivery_lon=delivery_lon,
            package_weight_lbs=round(random.uniform(0.5, 50.0), 1),
            created_at=now,
            estimated_delivery=now + timedelta(hours=random.uniform(2, 8)),
            status=DeliveryStatus.ORDER_PLACED
        )
        
    def _create_event(self, order: Order, event_type: DeliveryStatus, 
                      lat: float, lon: float, notes: str = "") -> DeliveryEvent:
        """Create a delivery event for an order."""
        now = datetime.utcnow()
        
        # Calculate delay
        delay_minutes = 0
        actual_delivery = None
        if event_type == DeliveryStatus.DELIVERED:
            actual_delivery = now.isoformat() + "Z"
            delay_minutes = int((now - order.estimated_delivery).total_seconds() / 60)
            delay_minutes = max(0, delay_minutes)  # Only positive delays
            
        return DeliveryEvent(
            event_id=str(uuid.uuid4()),
            delivery_id=order.delivery_id or f"DEL-{uuid.uuid4().hex[:8].upper()}",
            order_id=order.order_id,
            vehicle_id=order.vehicle_id or "",
            driver_id=order.driver_id or "",
            event_type=event_type.value,
            timestamp=now.isoformat() + "Z",
            latitude=round(lat, 6),
            longitude=round(lon, 6),
            customer_id=order.customer_id,
            package_weight_lbs=order.package_weight_lbs,
            estimated_delivery_time=order.estimated_delivery.isoformat() + "Z",
            actual_delivery_time=actual_delivery,
            delay_minutes=delay_minutes,
            notes=notes
        )
        
    def _advance_order(self, order: Order) -> Optional[DeliveryEvent]:
        """Advance an order to its next status and generate event."""
        current_status = order.status
        
        # State machine for order progression
        if current_status == DeliveryStatus.ORDER_PLACED:
            order.status = DeliveryStatus.ORDER_CONFIRMED
            return self._create_event(order, order.status, 
                                      order.pickup_lat, order.pickup_lon)
            
        elif current_status == DeliveryStatus.ORDER_CONFIRMED:
            # Assign vehicle and driver
            order.vehicle_id = random.choice(self.vehicle_ids)
            order.driver_id = random.choice(self.driver_ids)
            order.delivery_id = f"DEL-{uuid.uuid4().hex[:8].upper()}"
            order.status = DeliveryStatus.PICKED_UP
            return self._create_event(order, order.status,
                                      order.pickup_lat, order.pickup_lon)
            
        elif current_status == DeliveryStatus.PICKED_UP:
            order.status = DeliveryStatus.IN_TRANSIT
            # Midpoint location
            mid_lat = (order.pickup_lat + order.delivery_lat) / 2
            mid_lon = (order.pickup_lon + order.delivery_lon) / 2
            return self._create_event(order, order.status, mid_lat, mid_lon)
            
        elif current_status == DeliveryStatus.IN_TRANSIT:
            order.status = DeliveryStatus.OUT_FOR_DELIVERY
            # Near delivery location
            near_lat = order.delivery_lat + random.uniform(-0.01, 0.01)
            near_lon = order.delivery_lon + random.uniform(-0.01, 0.01)
            return self._create_event(order, order.status, near_lat, near_lon)
            
        elif current_status == DeliveryStatus.OUT_FOR_DELIVERY:
            # 95% success rate
            if random.random() < 0.95:
                order.status = DeliveryStatus.DELIVERED
                notes = random.choice(self.DELIVERY_NOTES)
            else:
                order.status = DeliveryStatus.FAILED_ATTEMPT
                notes = random.choice(self.FAILED_NOTES)
            return self._create_event(order, order.status,
                                      order.delivery_lat, order.delivery_lon, notes)
            
        elif current_status == DeliveryStatus.FAILED_ATTEMPT:
            # Retry or return
            if random.random() < 0.7:
                order.status = DeliveryStatus.OUT_FOR_DELIVERY
            else:
                order.status = DeliveryStatus.RETURNED
            return self._create_event(order, order.status,
                                      order.delivery_lat, order.delivery_lon)
            
        return None
        
    def generate_events(self, duration_seconds: int = 3600, interval_seconds: float = 1.0):
        """
        Generate delivery events for the specified duration.
        
        Args:
            duration_seconds: How long to run the simulation
            interval_seconds: Time between event batches
            
        Yields:
            DeliveryEvent objects
        """
        start_time = time.time()
        orders_per_second = self.orders_per_hour / 3600
        
        while (time.time() - start_time) < duration_seconds:
            # Generate new orders
            if random.random() < orders_per_second * interval_seconds:
                new_order = self._generate_order()
                self.active_orders[new_order.order_id] = new_order
                yield self._create_event(new_order, DeliveryStatus.ORDER_PLACED,
                                        new_order.pickup_lat, new_order.pickup_lon)
            
            # Advance existing orders
            orders_to_remove = []
            for order_id, order in self.active_orders.items():
                # Random chance to advance (simulates time passing)
                if random.random() < 0.1:
                    event = self._advance_order(order)
                    if event:
                        yield event
                        
                    # Check if order is complete
                    if order.status in [DeliveryStatus.DELIVERED, DeliveryStatus.RETURNED]:
                        orders_to_remove.append(order_id)
                        
            # Clean up completed orders
            for order_id in orders_to_remove:
                self.completed_orders.append(order_id)
                del self.active_orders[order_id]
                
            time.sleep(interval_seconds)
            
    def generate_batch(self, batch_size: int = 50) -> List[DeliveryEvent]:
        """Generate a batch of delivery events."""
        events = []
        
        # Add some new orders
        for _ in range(batch_size // 5):
            new_order = self._generate_order()
            self.active_orders[new_order.order_id] = new_order
            events.append(self._create_event(new_order, DeliveryStatus.ORDER_PLACED,
                                            new_order.pickup_lat, new_order.pickup_lon))
        
        # Advance existing orders
        for order in list(self.active_orders.values())[:batch_size]:
            event = self._advance_order(order)
            if event:
                events.append(event)
                
        return events


def main():
    """Run the delivery simulator in standalone mode."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Delivery Event Simulator")
    parser.add_argument("--orders-per-hour", type=int, default=500, help="Orders per hour")
    parser.add_argument("--vehicles", type=int, default=100, help="Number of vehicles")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")
    parser.add_argument("--interval", type=float, default=1.0, help="Event interval")
    parser.add_argument("--output", type=str, default=None, help="Output file (JSON lines)")
    
    args = parser.parse_args()
    
    simulator = DeliverySimulator(
        orders_per_hour=args.orders_per_hour,
        num_vehicles=args.vehicles
    )
    
    print(f"Starting delivery simulation for {args.duration} seconds...")
    
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
            
    print(f"\nGenerated {event_count} delivery events.")


if __name__ == "__main__":
    main()
