#!/usr/bin/env python3
"""
Smart Resource Allocator Agent
==============================

Dynamically allocates ETL resources based on:
1. Historical run patterns (duration, cost, memory usage)
2. Weekday vs weekend patterns
3. Input data volume estimation
4. Job complexity analysis
5. Time-of-day patterns
6. Trend analysis (growing/shrinking data)

This agent overrides static configurations with intelligent recommendations
to optimize cost and performance.

Usage:
    allocator = ResourceAllocatorAgent()
    recommendation = allocator.recommend_resources(
        job_name="sales_analytics",
        config=user_config,
        estimated_records=500000
    )
    # Use recommendation.optimized_config instead of user_config
"""

import os
import json
import math
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

# Import local storage
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from framework.storage import get_store


class DayType(Enum):
    """Day classification for pattern detection."""
    WEEKDAY = "weekday"
    WEEKEND = "weekend"
    MONTH_END = "month_end"
    QUARTER_END = "quarter_end"


class TimeOfDay(Enum):
    """Time of day classification."""
    MORNING = "morning"      # 6am - 12pm
    AFTERNOON = "afternoon"  # 12pm - 6pm
    EVENING = "evening"      # 6pm - 12am
    NIGHT = "night"          # 12am - 6am


class DataTrend(Enum):
    """Data volume trend."""
    GROWING = "growing"
    STABLE = "stable"
    SHRINKING = "shrinking"
    VOLATILE = "volatile"


@dataclass
class PatternAnalysis:
    """Analysis of historical patterns."""
    day_type: DayType
    time_of_day: TimeOfDay
    data_trend: DataTrend
    avg_records_weekday: float
    avg_records_weekend: float
    avg_duration_weekday: float
    avg_duration_weekend: float
    growth_rate_percent: float  # Weekly growth rate
    volatility_score: float     # 0-1, higher = more volatile
    peak_hours: List[int]       # Hours with highest load
    recommended_schedule: str   # Suggested cron expression


@dataclass
class ComplexityScore:
    """Job complexity assessment."""
    overall_score: float        # 1-10
    join_complexity: float      # Number and type of joins
    aggregation_complexity: float
    transformation_complexity: float
    data_skew_risk: float
    memory_pressure: float
    shuffle_intensity: float
    recommendations: List[str]


@dataclass
class ResourceRecommendation:
    """Recommended resource allocation."""
    job_name: str
    timestamp: str

    # Original config
    original_workers: int
    original_worker_type: str
    original_timeout: int

    # Recommended config
    recommended_workers: int
    recommended_worker_type: str
    recommended_timeout: int
    recommended_max_retries: int

    # Full optimized config
    optimized_config: Dict[str, Any]

    # Analysis
    pattern_analysis: PatternAnalysis
    complexity_score: ComplexityScore
    estimated_records: int
    estimated_duration_seconds: float
    estimated_cost: float

    # Comparison
    cost_savings_percent: float
    performance_improvement_percent: float

    # Reasoning
    allocation_reasons: List[str]
    warnings: List[str]
    confidence: float


class ResourceAllocatorAgent:
    """
    Smart resource allocation agent that dynamically recommends
    optimal resources based on patterns and predictions.
    """

    # Glue worker specifications
    WORKER_SPECS = {
        "Standard": {"memory_gb": 16, "vcpus": 4, "cost_per_hour": 0.44},
        "G.1X": {"memory_gb": 16, "vcpus": 4, "cost_per_hour": 0.44},
        "G.2X": {"memory_gb": 32, "vcpus": 8, "cost_per_hour": 0.88},
        "G.4X": {"memory_gb": 64, "vcpus": 16, "cost_per_hour": 1.76},
        "G.8X": {"memory_gb": 128, "vcpus": 32, "cost_per_hour": 3.52},
        "Z.2X": {"memory_gb": 64, "vcpus": 8, "cost_per_hour": 0.94},  # Memory optimized
    }

    # Thresholds for scaling
    RECORDS_PER_WORKER_STANDARD = 100000   # G.1X can handle ~100k records efficiently
    RECORDS_PER_WORKER_2X = 250000         # G.2X can handle ~250k records
    RECORDS_PER_WORKER_4X = 500000         # G.4X can handle ~500k records

    # Memory thresholds (GB per million records)
    MEMORY_PER_MILLION_RECORDS = 8  # Base memory needed

    # Complexity multipliers
    COMPLEXITY_MULTIPLIERS = {
        "simple": 1.0,
        "moderate": 1.3,
        "complex": 1.6,
        "very_complex": 2.0
    }

    def __init__(self):
        self.store = get_store()
        self.storage_path = "data/agent_store/resource_allocations.json"
        self._ensure_storage()

    def _ensure_storage(self):
        """Ensure storage file exists."""
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
        if not os.path.exists(self.storage_path):
            with open(self.storage_path, 'w') as f:
                json.dump({"allocations": [], "patterns": {}}, f)

    def recommend_resources(
        self,
        job_name: str,
        config: Dict[str, Any],
        estimated_records: Optional[int] = None,
        run_date: Optional[datetime] = None,
        force_analysis: bool = False
    ) -> ResourceRecommendation:
        """
        Recommend optimal resources for a job run.

        Args:
            job_name: Name of the ETL job
            config: User's original configuration
            estimated_records: Optional record count estimate (auto-detected if not provided)
            run_date: When the job will run (defaults to now)
            force_analysis: Force fresh analysis even if cached

        Returns:
            ResourceRecommendation with optimized configuration
        """
        run_date = run_date or datetime.now()

        # Extract original config
        original_workers = config.get('NumberOfWorkers', 10)
        original_worker_type = config.get('WorkerType', 'G.1X')
        original_timeout = config.get('Timeout', 480)

        # Analyze patterns
        pattern_analysis = self._analyze_patterns(job_name, run_date)

        # Analyze complexity
        complexity_score = self._analyze_complexity(job_name, config)

        # Estimate records if not provided
        if estimated_records is None:
            estimated_records = self._estimate_records(job_name, pattern_analysis, run_date)

        # Calculate optimal resources
        recommended_workers, recommended_worker_type = self._calculate_optimal_workers(
            estimated_records,
            complexity_score,
            pattern_analysis
        )

        # Calculate timeout based on predictions
        recommended_timeout = self._calculate_timeout(
            estimated_records,
            recommended_workers,
            recommended_worker_type,
            complexity_score,
            pattern_analysis
        )

        # Build optimized config
        optimized_config = self._build_optimized_config(
            config,
            recommended_workers,
            recommended_worker_type,
            recommended_timeout
        )

        # Estimate cost and duration
        estimated_duration = self._estimate_duration(
            estimated_records,
            recommended_workers,
            recommended_worker_type,
            complexity_score
        )
        estimated_cost = self._estimate_cost(
            recommended_workers,
            recommended_worker_type,
            estimated_duration
        )

        # Calculate original cost for comparison
        original_duration = self._estimate_duration(
            estimated_records,
            original_workers,
            original_worker_type,
            complexity_score
        )
        original_cost = self._estimate_cost(
            original_workers,
            original_worker_type,
            original_duration
        )

        # Calculate improvements
        cost_savings = ((original_cost - estimated_cost) / original_cost * 100) if original_cost > 0 else 0
        perf_improvement = ((original_duration - estimated_duration) / original_duration * 100) if original_duration > 0 else 0

        # Generate reasoning
        allocation_reasons = self._generate_reasons(
            job_name,
            pattern_analysis,
            complexity_score,
            estimated_records,
            original_workers,
            recommended_workers,
            original_worker_type,
            recommended_worker_type
        )

        # Generate warnings
        warnings = self._generate_warnings(
            pattern_analysis,
            complexity_score,
            estimated_records,
            recommended_workers
        )

        # Calculate confidence
        confidence = self._calculate_confidence(job_name, pattern_analysis)

        recommendation = ResourceRecommendation(
            job_name=job_name,
            timestamp=datetime.now().isoformat(),
            original_workers=original_workers,
            original_worker_type=original_worker_type,
            original_timeout=original_timeout,
            recommended_workers=recommended_workers,
            recommended_worker_type=recommended_worker_type,
            recommended_timeout=recommended_timeout,
            recommended_max_retries=1 if confidence > 0.7 else 2,
            optimized_config=optimized_config,
            pattern_analysis=pattern_analysis,
            complexity_score=complexity_score,
            estimated_records=estimated_records,
            estimated_duration_seconds=estimated_duration,
            estimated_cost=estimated_cost,
            cost_savings_percent=cost_savings,
            performance_improvement_percent=perf_improvement,
            allocation_reasons=allocation_reasons,
            warnings=warnings,
            confidence=confidence
        )

        # Store recommendation
        self._store_recommendation(recommendation)

        return recommendation

    def _analyze_patterns(self, job_name: str, run_date: datetime) -> PatternAnalysis:
        """Analyze historical patterns for the job."""
        history = self.store.get_execution_history(job_name=job_name, limit=100)

        # Classify current run date
        day_type = self._classify_day(run_date)
        time_of_day = self._classify_time(run_date)

        # Default values
        defaults = PatternAnalysis(
            day_type=day_type,
            time_of_day=time_of_day,
            data_trend=DataTrend.STABLE,
            avg_records_weekday=100000,
            avg_records_weekend=80000,
            avg_duration_weekday=300,
            avg_duration_weekend=240,
            growth_rate_percent=0,
            volatility_score=0.3,
            peak_hours=[9, 10, 14, 15],
            recommended_schedule="0 2 * * *"  # 2 AM daily
        )

        if not history or len(history) < 3:
            return defaults

        # Separate weekday and weekend runs
        weekday_runs = []
        weekend_runs = []

        for run in history:
            try:
                ts = datetime.fromisoformat(run.get('timestamp', ''))
                if ts.weekday() < 5:
                    weekday_runs.append(run)
                else:
                    weekend_runs.append(run)
            except:
                weekday_runs.append(run)

        # Calculate averages
        def avg(runs, key, default=0):
            values = [r.get(key, default) for r in runs if r.get(key)]
            return statistics.mean(values) if values else default

        avg_records_weekday = avg(weekday_runs, 'records_processed', 100000)
        avg_records_weekend = avg(weekend_runs, 'records_processed', 80000)
        avg_duration_weekday = avg(weekday_runs, 'duration_seconds', 300)
        avg_duration_weekend = avg(weekend_runs, 'duration_seconds', 240)

        # Calculate data trend (compare recent to older)
        if len(history) >= 10:
            recent_records = [h.get('records_processed', 0) for h in history[:5]]
            older_records = [h.get('records_processed', 0) for h in history[5:10]]

            recent_avg = statistics.mean(recent_records) if recent_records else 0
            older_avg = statistics.mean(older_records) if older_records else 0

            if older_avg > 0:
                growth_rate = (recent_avg - older_avg) / older_avg * 100
            else:
                growth_rate = 0

            if growth_rate > 10:
                data_trend = DataTrend.GROWING
            elif growth_rate < -10:
                data_trend = DataTrend.SHRINKING
            else:
                data_trend = DataTrend.STABLE
        else:
            growth_rate = 0
            data_trend = DataTrend.STABLE

        # Calculate volatility
        all_records = [h.get('records_processed', 0) for h in history if h.get('records_processed')]
        if len(all_records) >= 3:
            mean_records = statistics.mean(all_records)
            std_records = statistics.stdev(all_records) if len(all_records) > 1 else 0
            volatility = min(1.0, std_records / mean_records if mean_records > 0 else 0)
            if volatility > 0.5:
                data_trend = DataTrend.VOLATILE
        else:
            volatility = 0.3

        # Detect peak hours
        hour_counts = {}
        for run in history:
            try:
                ts = datetime.fromisoformat(run.get('timestamp', ''))
                hour = ts.hour
                hour_counts[hour] = hour_counts.get(hour, 0) + 1
            except:
                pass

        peak_hours = sorted(hour_counts.keys(), key=lambda h: hour_counts[h], reverse=True)[:4]
        if not peak_hours:
            peak_hours = [9, 10, 14, 15]

        # Recommend schedule (run during off-peak)
        off_peak = [h for h in range(24) if h not in peak_hours]
        best_hour = off_peak[0] if off_peak else 2
        recommended_schedule = f"0 {best_hour} * * *"

        return PatternAnalysis(
            day_type=day_type,
            time_of_day=time_of_day,
            data_trend=data_trend,
            avg_records_weekday=avg_records_weekday,
            avg_records_weekend=avg_records_weekend,
            avg_duration_weekday=avg_duration_weekday,
            avg_duration_weekend=avg_duration_weekend,
            growth_rate_percent=growth_rate,
            volatility_score=volatility,
            peak_hours=peak_hours,
            recommended_schedule=recommended_schedule
        )

    def _analyze_complexity(self, job_name: str, config: Dict) -> ComplexityScore:
        """Analyze job complexity from config and history."""
        recommendations = []

        # Check for complexity indicators in config
        default_args = config.get('DefaultArguments', {})

        # Join complexity
        join_complexity = 3.0  # Default moderate
        if '--enable-auto-scaling' in str(default_args):
            join_complexity = 4.0

        # Aggregation complexity
        agg_complexity = 3.0

        # Check historical memory usage
        history = self.store.get_execution_history(job_name=job_name, limit=10)
        memory_usage = [h.get('memory_gb', 16) for h in history if h.get('memory_gb')]
        avg_memory = statistics.mean(memory_usage) if memory_usage else 16

        # Memory pressure
        worker_type = config.get('WorkerType', 'G.1X')
        worker_memory = self.WORKER_SPECS.get(worker_type, {}).get('memory_gb', 16)
        memory_pressure = min(10, (avg_memory / worker_memory) * 5)

        # Shuffle intensity (estimate from duration variance)
        durations = [h.get('duration_seconds', 300) for h in history if h.get('duration_seconds')]
        if len(durations) > 1:
            duration_variance = statistics.stdev(durations) / statistics.mean(durations)
            shuffle_intensity = min(10, duration_variance * 10)
        else:
            shuffle_intensity = 3.0

        # Overall score
        overall = (join_complexity + agg_complexity + memory_pressure + shuffle_intensity) / 4

        # Generate recommendations
        if memory_pressure > 5:
            recommendations.append("High memory usage detected - consider G.2X or higher worker type")
        if shuffle_intensity > 5:
            recommendations.append("High shuffle detected - enable adaptive query execution")
        if overall > 6:
            recommendations.append("Complex job - consider breaking into stages")

        return ComplexityScore(
            overall_score=overall,
            join_complexity=join_complexity,
            aggregation_complexity=agg_complexity,
            transformation_complexity=3.0,
            data_skew_risk=3.0,
            memory_pressure=memory_pressure,
            shuffle_intensity=shuffle_intensity,
            recommendations=recommendations
        )

    def _estimate_records(
        self,
        job_name: str,
        pattern_analysis: PatternAnalysis,
        run_date: datetime
    ) -> int:
        """Estimate record count for the run."""
        # Base on day type
        if pattern_analysis.day_type == DayType.WEEKEND:
            base_records = pattern_analysis.avg_records_weekend
        else:
            base_records = pattern_analysis.avg_records_weekday

        # Adjust for trend
        if pattern_analysis.data_trend == DataTrend.GROWING:
            growth_factor = 1 + (pattern_analysis.growth_rate_percent / 100)
            base_records *= growth_factor
        elif pattern_analysis.data_trend == DataTrend.SHRINKING:
            shrink_factor = 1 + (pattern_analysis.growth_rate_percent / 100)  # negative
            base_records *= max(0.5, shrink_factor)

        # Adjust for month-end (typically higher volume)
        if run_date.day >= 28:
            base_records *= 1.3

        # Adjust for quarter-end
        if run_date.month in [3, 6, 9, 12] and run_date.day >= 28:
            base_records *= 1.5

        return int(base_records)

    def _calculate_optimal_workers(
        self,
        estimated_records: int,
        complexity: ComplexityScore,
        patterns: PatternAnalysis
    ) -> Tuple[int, str]:
        """Calculate optimal worker count and type."""

        # Determine worker type based on complexity and records
        if complexity.memory_pressure > 6 or estimated_records > 2000000:
            worker_type = "G.2X"
            records_per_worker = self.RECORDS_PER_WORKER_2X
        elif complexity.memory_pressure > 8 or estimated_records > 5000000:
            worker_type = "G.4X"
            records_per_worker = self.RECORDS_PER_WORKER_4X
        else:
            worker_type = "G.1X"
            records_per_worker = self.RECORDS_PER_WORKER_STANDARD

        # Apply complexity multiplier
        complexity_level = "simple"
        if complexity.overall_score > 7:
            complexity_level = "very_complex"
        elif complexity.overall_score > 5:
            complexity_level = "complex"
        elif complexity.overall_score > 3:
            complexity_level = "moderate"

        multiplier = self.COMPLEXITY_MULTIPLIERS[complexity_level]
        adjusted_records_per_worker = records_per_worker / multiplier

        # Calculate base workers
        base_workers = math.ceil(estimated_records / adjusted_records_per_worker)

        # Add buffer for volatility
        if patterns.volatility_score > 0.5:
            base_workers = int(base_workers * 1.2)

        # Add buffer for growing trend
        if patterns.data_trend == DataTrend.GROWING:
            base_workers = int(base_workers * 1.1)

        # Enforce limits
        workers = max(2, min(100, base_workers))

        return workers, worker_type

    def _calculate_timeout(
        self,
        estimated_records: int,
        workers: int,
        worker_type: str,
        complexity: ComplexityScore,
        patterns: PatternAnalysis
    ) -> int:
        """Calculate optimal timeout in minutes."""
        # Base duration estimate (seconds)
        records_per_second = 1000  # Base throughput
        worker_specs = self.WORKER_SPECS.get(worker_type, self.WORKER_SPECS["G.1X"])

        # Adjust for worker type
        if worker_type == "G.2X":
            records_per_second *= 1.5
        elif worker_type == "G.4X":
            records_per_second *= 2.5

        # Calculate base duration
        total_throughput = records_per_second * workers
        base_duration_seconds = estimated_records / total_throughput if total_throughput > 0 else 300

        # Apply complexity multiplier
        base_duration_seconds *= (1 + complexity.overall_score / 10)

        # Add startup/shutdown overhead
        base_duration_seconds += 120  # 2 minutes overhead

        # Convert to minutes and add buffer
        base_duration_minutes = base_duration_seconds / 60

        # Add 50% buffer for safety
        timeout = int(base_duration_minutes * 1.5)

        # Add more buffer for volatile patterns
        if patterns.volatility_score > 0.5:
            timeout = int(timeout * 1.3)

        # Enforce limits (min 10 min, max 2880 min = 48 hours)
        return max(10, min(2880, timeout))

    def _build_optimized_config(
        self,
        original_config: Dict,
        workers: int,
        worker_type: str,
        timeout: int
    ) -> Dict[str, Any]:
        """Build optimized configuration."""
        config = original_config.copy()

        # Update resource settings
        config['NumberOfWorkers'] = workers
        config['WorkerType'] = worker_type
        config['Timeout'] = timeout

        # Add optimization settings
        default_args = config.get('DefaultArguments', {})
        default_args['--enable-auto-scaling'] = 'true'
        default_args['--enable-metrics'] = 'true'
        default_args['--enable-continuous-cloudwatch-log'] = 'true'

        # Add Spark optimizations
        default_args['--conf'] = 'spark.sql.adaptive.enabled=true'

        config['DefaultArguments'] = default_args

        return config

    def _estimate_duration(
        self,
        records: int,
        workers: int,
        worker_type: str,
        complexity: ComplexityScore
    ) -> float:
        """Estimate job duration in seconds."""
        records_per_second = 1000

        if worker_type == "G.2X":
            records_per_second *= 1.5
        elif worker_type == "G.4X":
            records_per_second *= 2.5

        total_throughput = records_per_second * workers
        base_duration = records / total_throughput if total_throughput > 0 else 300

        # Apply complexity
        duration = base_duration * (1 + complexity.overall_score / 10)

        # Add overhead
        duration += 120

        return duration

    def _estimate_cost(
        self,
        workers: int,
        worker_type: str,
        duration_seconds: float
    ) -> float:
        """Estimate job cost in USD."""
        specs = self.WORKER_SPECS.get(worker_type, self.WORKER_SPECS["G.1X"])
        cost_per_hour = specs['cost_per_hour']

        duration_hours = duration_seconds / 3600
        cost = workers * cost_per_hour * duration_hours

        return round(cost, 2)

    def _classify_day(self, dt: datetime) -> DayType:
        """Classify the day type."""
        # Check quarter end
        if dt.month in [3, 6, 9, 12] and dt.day >= 28:
            return DayType.QUARTER_END

        # Check month end
        if dt.day >= 28:
            return DayType.MONTH_END

        # Check weekend
        if dt.weekday() >= 5:
            return DayType.WEEKEND

        return DayType.WEEKDAY

    def _classify_time(self, dt: datetime) -> TimeOfDay:
        """Classify time of day."""
        hour = dt.hour
        if 6 <= hour < 12:
            return TimeOfDay.MORNING
        elif 12 <= hour < 18:
            return TimeOfDay.AFTERNOON
        elif 18 <= hour < 24:
            return TimeOfDay.EVENING
        else:
            return TimeOfDay.NIGHT

    def _generate_reasons(
        self,
        job_name: str,
        patterns: PatternAnalysis,
        complexity: ComplexityScore,
        records: int,
        orig_workers: int,
        rec_workers: int,
        orig_type: str,
        rec_type: str
    ) -> List[str]:
        """Generate allocation reasoning."""
        reasons = []

        # Day type reasoning
        if patterns.day_type == DayType.WEEKEND:
            reasons.append(f"Weekend run: Lower volume expected ({patterns.avg_records_weekend:,.0f} vs {patterns.avg_records_weekday:,.0f} weekday avg)")
        elif patterns.day_type == DayType.MONTH_END:
            reasons.append("Month-end run: Higher volume expected (+30%)")
        elif patterns.day_type == DayType.QUARTER_END:
            reasons.append("Quarter-end run: Higher volume expected (+50%)")

        # Trend reasoning
        if patterns.data_trend == DataTrend.GROWING:
            reasons.append(f"Data trend: GROWING at {patterns.growth_rate_percent:.1f}% weekly")
        elif patterns.data_trend == DataTrend.SHRINKING:
            reasons.append(f"Data trend: SHRINKING at {abs(patterns.growth_rate_percent):.1f}% weekly")
        elif patterns.data_trend == DataTrend.VOLATILE:
            reasons.append(f"Data trend: VOLATILE (score: {patterns.volatility_score:.2f}) - added buffer")

        # Worker changes
        if rec_workers != orig_workers:
            if rec_workers > orig_workers:
                reasons.append(f"Increased workers {orig_workers} → {rec_workers}: Higher volume ({records:,} records)")
            else:
                reasons.append(f"Reduced workers {orig_workers} → {rec_workers}: Lower volume ({records:,} records)")

        # Worker type changes
        if rec_type != orig_type:
            reasons.append(f"Changed worker type {orig_type} → {rec_type}: Memory pressure score {complexity.memory_pressure:.1f}/10")

        # Complexity reasoning
        if complexity.overall_score > 5:
            reasons.append(f"High complexity job (score: {complexity.overall_score:.1f}/10)")

        return reasons

    def _generate_warnings(
        self,
        patterns: PatternAnalysis,
        complexity: ComplexityScore,
        records: int,
        workers: int
    ) -> List[str]:
        """Generate warnings."""
        warnings = []

        if patterns.volatility_score > 0.7:
            warnings.append("High volatility in data volume - monitor closely")

        if complexity.memory_pressure > 7:
            warnings.append("High memory pressure - consider vertical scaling")

        if complexity.shuffle_intensity > 7:
            warnings.append("High shuffle intensity - potential for data skew")

        if records > 5000000 and workers < 20:
            warnings.append("Large data volume with few workers - may run long")

        if patterns.data_trend == DataTrend.GROWING and patterns.growth_rate_percent > 20:
            warnings.append(f"Rapid data growth ({patterns.growth_rate_percent:.1f}%/week) - plan capacity")

        return warnings

    def _calculate_confidence(self, job_name: str, patterns: PatternAnalysis) -> float:
        """Calculate confidence in the recommendation."""
        history = self.store.get_execution_history(job_name=job_name, limit=100)

        # Base confidence on history length
        if len(history) >= 20:
            confidence = 0.9
        elif len(history) >= 10:
            confidence = 0.8
        elif len(history) >= 5:
            confidence = 0.7
        else:
            confidence = 0.5

        # Reduce for high volatility
        confidence -= patterns.volatility_score * 0.2

        # Ensure bounds
        return max(0.3, min(0.95, confidence))

    def _store_recommendation(self, rec: ResourceRecommendation):
        """Store recommendation for learning."""
        try:
            with open(self.storage_path, 'r') as f:
                data = json.load(f)
        except:
            data = {"allocations": [], "patterns": {}}

        record = {
            "job_name": rec.job_name,
            "timestamp": rec.timestamp,
            "original": {
                "workers": rec.original_workers,
                "worker_type": rec.original_worker_type
            },
            "recommended": {
                "workers": rec.recommended_workers,
                "worker_type": rec.recommended_worker_type
            },
            "estimated_records": rec.estimated_records,
            "estimated_cost": rec.estimated_cost,
            "cost_savings_percent": rec.cost_savings_percent,
            "confidence": rec.confidence
        }

        data["allocations"].append(record)

        # Keep last 100
        data["allocations"] = data["allocations"][-100:]

        with open(self.storage_path, 'w') as f:
            json.dump(data, f, indent=2)

    def get_allocation_history(self, job_name: str = None, limit: int = 10) -> List[Dict]:
        """Get allocation history."""
        try:
            with open(self.storage_path, 'r') as f:
                data = json.load(f)

            allocations = data.get("allocations", [])

            if job_name:
                allocations = [a for a in allocations if a.get("job_name") == job_name]

            return allocations[-limit:]
        except:
            return []

    def generate_report(self, rec: ResourceRecommendation) -> str:
        """Generate human-readable report."""
        report = []
        report.append("=" * 70)
        report.append("SMART RESOURCE ALLOCATION REPORT")
        report.append("=" * 70)

        report.append(f"\nJob: {rec.job_name}")
        report.append(f"Timestamp: {rec.timestamp}")
        report.append(f"Confidence: {rec.confidence * 100:.0f}%")

        report.append("\n--- Pattern Analysis ---")
        report.append(f"  Day Type: {rec.pattern_analysis.day_type.value}")
        report.append(f"  Data Trend: {rec.pattern_analysis.data_trend.value}")
        report.append(f"  Growth Rate: {rec.pattern_analysis.growth_rate_percent:+.1f}%/week")
        report.append(f"  Volatility: {rec.pattern_analysis.volatility_score:.2f}")

        report.append("\n--- Resource Recommendation ---")
        report.append(f"  Original:    {rec.original_workers} x {rec.original_worker_type}")
        report.append(f"  Recommended: {rec.recommended_workers} x {rec.recommended_worker_type}")
        report.append(f"  Timeout:     {rec.recommended_timeout} minutes")

        report.append("\n--- Estimates ---")
        report.append(f"  Records:  {rec.estimated_records:,}")
        report.append(f"  Duration: {rec.estimated_duration_seconds / 60:.1f} minutes")
        report.append(f"  Cost:     ${rec.estimated_cost:.2f}")

        report.append("\n--- Improvements ---")
        savings_color = "↓" if rec.cost_savings_percent > 0 else "↑"
        report.append(f"  Cost:        {savings_color} {abs(rec.cost_savings_percent):.1f}%")
        perf_color = "↑" if rec.performance_improvement_percent > 0 else "↓"
        report.append(f"  Performance: {perf_color} {abs(rec.performance_improvement_percent):.1f}%")

        if rec.allocation_reasons:
            report.append("\n--- Allocation Reasons ---")
            for reason in rec.allocation_reasons:
                report.append(f"  • {reason}")

        if rec.warnings:
            report.append("\n--- Warnings ---")
            for warning in rec.warnings:
                report.append(f"  ⚠ {warning}")

        report.append("\n" + "=" * 70)

        return "\n".join(report)
