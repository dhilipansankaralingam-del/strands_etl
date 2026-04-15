#!/usr/bin/env python3
"""
Recommendation Agent
====================

Intelligent agent that aggregates recommendations from all other agents:
1. Consolidates recommendations from Code Analysis, Workload Assessment, etc.
2. Prioritizes recommendations based on impact and effort
3. Generates actionable implementation plans
4. Tracks recommendation history and outcomes
5. Provides cost-benefit analysis for each recommendation

Serves as the single source of truth for all optimization suggestions.
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime


class RecommendationSource(Enum):
    """Sources of recommendations."""
    CODE_ANALYSIS = "code_analysis"
    WORKLOAD_ASSESSMENT = "workload_assessment"
    DATA_QUALITY = "data_quality"
    COMPLIANCE = "compliance"
    LEARNING = "learning"
    AUTO_HEALING = "auto_healing"
    MANUAL = "manual"


class RecommendationPriority(Enum):
    """Priority levels for recommendations."""
    CRITICAL = "critical"  # Must fix - blocking issues
    HIGH = "high"  # Should fix soon - significant impact
    MEDIUM = "medium"  # Fix when possible - moderate impact
    LOW = "low"  # Nice to have - minor impact
    INFO = "info"  # Informational only


class ImplementationEffort(Enum):
    """Effort required to implement."""
    TRIVIAL = "trivial"  # Minutes, config change
    EASY = "easy"  # Hours, simple code change
    MODERATE = "moderate"  # Days, significant changes
    HARD = "hard"  # Weeks, major refactoring
    COMPLEX = "complex"  # Extensive work required


class RecommendationStatus(Enum):
    """Status of recommendation."""
    NEW = "new"
    ACKNOWLEDGED = "acknowledged"
    IN_PROGRESS = "in_progress"
    IMPLEMENTED = "implemented"
    REJECTED = "rejected"
    DEFERRED = "deferred"


@dataclass
class Recommendation:
    """A single recommendation."""
    id: str
    source: RecommendationSource
    priority: RecommendationPriority
    title: str
    description: str
    impact: str
    implementation: str
    effort: ImplementationEffort
    category: str
    affected_component: str = ""
    estimated_benefit: str = ""
    code_snippet: str = ""
    config_changes: Dict[str, str] = field(default_factory=dict)
    related_recommendations: List[str] = field(default_factory=list)
    status: RecommendationStatus = RecommendationStatus.NEW
    created_at: str = ""
    updated_at: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecommendationPlan:
    """Implementation plan for recommendations."""
    job_name: str
    timestamp: str
    total_recommendations: int
    by_priority: Dict[str, int] = field(default_factory=dict)
    by_source: Dict[str, int] = field(default_factory=dict)
    quick_wins: List[Recommendation] = field(default_factory=list)
    high_impact: List[Recommendation] = field(default_factory=list)
    all_recommendations: List[Recommendation] = field(default_factory=list)
    implementation_order: List[str] = field(default_factory=list)
    estimated_total_effort: str = ""
    estimated_total_benefit: str = ""


class RecommendationAgent:
    """
    Agent that aggregates and prioritizes recommendations from all sources.
    """

    def __init__(self, config, dynamodb_client=None):
        self.config = config
        self.dynamodb = dynamodb_client
        self.recommendations_table = getattr(config, 'recommendations_table', 'etl_recommendations')

        # Priority weights for scoring
        self.priority_weights = {
            RecommendationPriority.CRITICAL: 100,
            RecommendationPriority.HIGH: 75,
            RecommendationPriority.MEDIUM: 50,
            RecommendationPriority.LOW: 25,
            RecommendationPriority.INFO: 10
        }

        # Effort costs for scoring
        self.effort_costs = {
            ImplementationEffort.TRIVIAL: 1,
            ImplementationEffort.EASY: 2,
            ImplementationEffort.MODERATE: 5,
            ImplementationEffort.HARD: 10,
            ImplementationEffort.COMPLEX: 20
        }

    def aggregate_recommendations(
        self,
        code_analysis_results: Optional[Dict] = None,
        workload_assessment: Optional[Dict] = None,
        dq_report: Optional[Dict] = None,
        compliance_result: Optional[Dict] = None,
        learning_result: Optional[Dict] = None,
        healing_result: Optional[Dict] = None,
        job_name: str = ""
    ) -> RecommendationPlan:
        """
        Aggregate recommendations from all agent outputs.

        Args:
            code_analysis_results: Output from CodeAnalysisAgent
            workload_assessment: Output from WorkloadAssessmentAgent
            dq_report: Output from DataQualityAgent
            compliance_result: Output from ComplianceAgent
            learning_result: Output from LearningAgent
            healing_result: Output from AutoHealingAgent
            job_name: Name of the job

        Returns:
            RecommendationPlan with all recommendations prioritized
        """
        all_recommendations = []
        timestamp = datetime.utcnow().isoformat()

        # Process code analysis recommendations
        if code_analysis_results:
            all_recommendations.extend(
                self._process_code_analysis(code_analysis_results)
            )

        # Process workload assessment recommendations
        if workload_assessment:
            all_recommendations.extend(
                self._process_workload_assessment(workload_assessment)
            )

        # Process data quality recommendations
        if dq_report:
            all_recommendations.extend(
                self._process_dq_report(dq_report)
            )

        # Process compliance recommendations
        if compliance_result:
            all_recommendations.extend(
                self._process_compliance(compliance_result)
            )

        # Process learning recommendations
        if learning_result:
            all_recommendations.extend(
                self._process_learning(learning_result)
            )

        # Process healing recommendations
        if healing_result:
            all_recommendations.extend(
                self._process_healing(healing_result)
            )

        # Score and sort recommendations
        scored_recommendations = self._score_recommendations(all_recommendations)

        # Build plan
        plan = RecommendationPlan(
            job_name=job_name,
            timestamp=timestamp,
            total_recommendations=len(scored_recommendations),
            all_recommendations=scored_recommendations
        )

        # Categorize
        plan.by_priority = self._count_by_priority(scored_recommendations)
        plan.by_source = self._count_by_source(scored_recommendations)

        # Identify quick wins (high priority, low effort)
        plan.quick_wins = [
            r for r in scored_recommendations
            if r.priority in [RecommendationPriority.CRITICAL, RecommendationPriority.HIGH]
            and r.effort in [ImplementationEffort.TRIVIAL, ImplementationEffort.EASY]
        ][:5]

        # Identify high impact items
        plan.high_impact = [
            r for r in scored_recommendations
            if r.priority in [RecommendationPriority.CRITICAL, RecommendationPriority.HIGH]
        ][:10]

        # Generate implementation order
        plan.implementation_order = self._generate_implementation_order(scored_recommendations)

        # Estimate totals
        plan.estimated_total_effort = self._estimate_total_effort(scored_recommendations)
        plan.estimated_total_benefit = self._estimate_total_benefit(scored_recommendations)

        return plan

    def _process_code_analysis(self, results: Dict) -> List[Recommendation]:
        """Process code analysis results into recommendations."""
        recommendations = []

        for rec in results.get("recommendations", []):
            severity = rec.get("severity", "medium").lower()
            priority = self._map_severity_to_priority(severity)

            recommendations.append(Recommendation(
                id=f"CODE_{len(recommendations):04d}",
                source=RecommendationSource.CODE_ANALYSIS,
                priority=priority,
                title=rec.get("title", "Code Optimization"),
                description=rec.get("description", ""),
                impact=rec.get("estimated_impact", "Performance improvement"),
                implementation=rec.get("suggestion", ""),
                effort=self._estimate_effort_from_code(rec),
                category=rec.get("category", "performance"),
                code_snippet=rec.get("suggested_code", ""),
                created_at=datetime.utcnow().isoformat()
            ))

        return recommendations

    def _process_workload_assessment(self, assessment: Dict) -> List[Recommendation]:
        """Process workload assessment into recommendations."""
        recommendations = []

        # From warnings
        for warning in assessment.get("warnings", []):
            recommendations.append(Recommendation(
                id=f"WORK_{len(recommendations):04d}",
                source=RecommendationSource.WORKLOAD_ASSESSMENT,
                priority=RecommendationPriority.MEDIUM,
                title="Workload Warning",
                description=warning,
                impact="May affect job stability or performance",
                implementation="Review and address the warning condition",
                effort=ImplementationEffort.MODERATE,
                category="workload",
                created_at=datetime.utcnow().isoformat()
            ))

        # From optimization opportunities
        for opt in assessment.get("optimization_opportunities", []):
            recommendations.append(Recommendation(
                id=f"WORK_{len(recommendations):04d}",
                source=RecommendationSource.WORKLOAD_ASSESSMENT,
                priority=RecommendationPriority.MEDIUM,
                title="Optimization Opportunity",
                description=opt,
                impact="Potential performance or cost improvement",
                implementation=opt,
                effort=ImplementationEffort.EASY,
                category="optimization",
                created_at=datetime.utcnow().isoformat()
            ))

        # From primary recommendation
        primary = assessment.get("primary_recommendation")
        if primary:
            config_changes = primary.get("spark_configs", {})
            if config_changes:
                recommendations.append(Recommendation(
                    id=f"WORK_{len(recommendations):04d}",
                    source=RecommendationSource.WORKLOAD_ASSESSMENT,
                    priority=RecommendationPriority.HIGH,
                    title="Apply Recommended Configuration",
                    description=f"Use {primary.get('platform')} with {primary.get('worker_type')} x {primary.get('num_workers')}",
                    impact=f"Estimated cost: ${primary.get('estimated_cost', 0):.2f}, duration: {primary.get('estimated_duration_minutes', 0)} min",
                    implementation="Update job configuration",
                    effort=ImplementationEffort.TRIVIAL,
                    category="configuration",
                    config_changes=config_changes,
                    created_at=datetime.utcnow().isoformat()
                ))

        return recommendations

    def _process_dq_report(self, report: Dict) -> List[Recommendation]:
        """Process data quality report into recommendations."""
        recommendations = []

        for dq_rec in report.get("recommendations", []):
            recommendations.append(Recommendation(
                id=f"DQ_{len(recommendations):04d}",
                source=RecommendationSource.DATA_QUALITY,
                priority=RecommendationPriority.MEDIUM,
                title="Data Quality Improvement",
                description=dq_rec,
                impact="Improved data quality and reliability",
                implementation=dq_rec,
                effort=ImplementationEffort.MODERATE,
                category="data_quality",
                created_at=datetime.utcnow().isoformat()
            ))

        # Add recommendations for failed rules
        for result in report.get("results", []):
            if result.get("status") == "failed":
                recommendations.append(Recommendation(
                    id=f"DQ_{len(recommendations):04d}",
                    source=RecommendationSource.DATA_QUALITY,
                    priority=RecommendationPriority.HIGH,
                    title=f"Fix DQ Rule: {result.get('rule_id')}",
                    description=result.get("rule_description", ""),
                    impact=f"Failed records: {result.get('records_failed', 0)}",
                    implementation="Add data validation or transformation to fix the issue",
                    effort=ImplementationEffort.MODERATE,
                    category="data_quality",
                    created_at=datetime.utcnow().isoformat()
                ))

        return recommendations

    def _process_compliance(self, result: Dict) -> List[Recommendation]:
        """Process compliance results into recommendations."""
        recommendations = []

        # PII findings
        for finding in result.get("pii_findings", []):
            recommendations.append(Recommendation(
                id=f"COMP_{len(recommendations):04d}",
                source=RecommendationSource.COMPLIANCE,
                priority=RecommendationPriority.HIGH,
                title=f"PII Detected: {finding.get('column_name')}",
                description=f"PII type: {finding.get('pii_type')}, Confidence: {finding.get('confidence', 0):.0%}",
                impact="Compliance risk if PII is not properly handled",
                implementation=finding.get("recommendation", "Apply masking or encryption"),
                effort=ImplementationEffort.EASY,
                category="compliance",
                created_at=datetime.utcnow().isoformat()
            ))

        # Violations
        for violation in result.get("violations", []):
            severity = violation.get("severity", "medium")
            priority = self._map_severity_to_priority(severity)

            recommendations.append(Recommendation(
                id=f"COMP_{len(recommendations):04d}",
                source=RecommendationSource.COMPLIANCE,
                priority=priority,
                title=f"Compliance Violation: {violation.get('rule_id')}",
                description=violation.get("rule_description", ""),
                impact=f"Framework: {violation.get('framework')}, Severity: {severity}",
                implementation=violation.get("remediation", ""),
                effort=ImplementationEffort.MODERATE,
                category="compliance",
                affected_component=violation.get("table_name", ""),
                created_at=datetime.utcnow().isoformat()
            ))

        # Masking recommendations
        for mask_rec in result.get("recommendations", []):
            if isinstance(mask_rec, str) and "mask" in mask_rec.lower():
                recommendations.append(Recommendation(
                    id=f"COMP_{len(recommendations):04d}",
                    source=RecommendationSource.COMPLIANCE,
                    priority=RecommendationPriority.MEDIUM,
                    title="Apply Data Masking",
                    description="Implement PII masking as recommended",
                    impact="Protect sensitive data",
                    implementation=mask_rec,
                    effort=ImplementationEffort.EASY,
                    category="compliance",
                    code_snippet=mask_rec if mask_rec.startswith("df") else "",
                    created_at=datetime.utcnow().isoformat()
                ))

        return recommendations

    def _process_learning(self, result: Dict) -> List[Recommendation]:
        """Process learning agent results into recommendations."""
        recommendations = []

        # From recommendations list
        for rec in result.get("recommendations", []):
            recommendations.append(Recommendation(
                id=f"LEARN_{len(recommendations):04d}",
                source=RecommendationSource.LEARNING,
                priority=RecommendationPriority.MEDIUM,
                title="Historical Insight",
                description=rec,
                impact="Based on execution history analysis",
                implementation=rec,
                effort=ImplementationEffort.MODERATE,
                category="learning",
                created_at=datetime.utcnow().isoformat()
            ))

        # From failure prediction
        prediction = result.get("failure_prediction")
        if prediction and prediction.get("probability", 0) > 0.3:
            for action in prediction.get("recommended_actions", []):
                recommendations.append(Recommendation(
                    id=f"LEARN_{len(recommendations):04d}",
                    source=RecommendationSource.LEARNING,
                    priority=RecommendationPriority.HIGH,
                    title="Failure Prevention",
                    description=f"Failure probability: {prediction.get('probability', 0):.0%}",
                    impact="Prevent potential job failure",
                    implementation=action,
                    effort=ImplementationEffort.EASY,
                    category="reliability",
                    metadata={"probability": prediction.get("probability")},
                    created_at=datetime.utcnow().isoformat()
                ))

        # From insights
        for insight in result.get("insights", []):
            if insight.get("impact", "").lower() == "high":
                recommendations.append(Recommendation(
                    id=f"LEARN_{len(recommendations):04d}",
                    source=RecommendationSource.LEARNING,
                    priority=RecommendationPriority.MEDIUM,
                    title=insight.get("title", "Insight"),
                    description=insight.get("description", ""),
                    impact=insight.get("impact", ""),
                    implementation=insight.get("action", ""),
                    effort=ImplementationEffort.MODERATE,
                    category=insight.get("category", "learning"),
                    metadata=insight.get("data", {}),
                    created_at=datetime.utcnow().isoformat()
                ))

        return recommendations

    def _process_healing(self, result: Dict) -> List[Recommendation]:
        """Process auto-healing results into recommendations."""
        recommendations = []

        if not result.get("can_heal", False):
            return recommendations

        # Strategy recommendations
        for strategy in result.get("strategies", []):
            recommendations.append(Recommendation(
                id=f"HEAL_{len(recommendations):04d}",
                source=RecommendationSource.AUTO_HEALING,
                priority=RecommendationPriority.HIGH,
                title=f"Healing Strategy: {strategy}",
                description=f"Recommended healing approach for the error",
                impact="Resolve current error and prevent recurrence",
                implementation=f"Apply {strategy} strategy",
                effort=ImplementationEffort.EASY,
                category="healing",
                created_at=datetime.utcnow().isoformat()
            ))

        # Code fixes
        for fix in result.get("code_fixes", []):
            recommendations.append(Recommendation(
                id=f"HEAL_{len(recommendations):04d}",
                source=RecommendationSource.AUTO_HEALING,
                priority=RecommendationPriority.HIGH,
                title=fix.get("description", "Code Fix"),
                description=f"Type: {fix.get('type')}",
                impact="Fix the current error",
                implementation="Apply the suggested code change",
                effort=ImplementationEffort.EASY,
                category="healing",
                code_snippet=fix.get("code", fix.get("code_template", "")),
                created_at=datetime.utcnow().isoformat()
            ))

        # Config changes
        config_changes = result.get("config_changes", {})
        if config_changes:
            recommendations.append(Recommendation(
                id=f"HEAL_{len(recommendations):04d}",
                source=RecommendationSource.AUTO_HEALING,
                priority=RecommendationPriority.HIGH,
                title="Apply Healing Configuration",
                description="Spark configuration changes to address the error",
                impact="Prevent similar errors",
                implementation="Update Spark configurations",
                effort=ImplementationEffort.TRIVIAL,
                category="healing",
                config_changes=config_changes,
                created_at=datetime.utcnow().isoformat()
            ))

        # General recommendations from healing
        for rec in result.get("recommendations", []):
            recommendations.append(Recommendation(
                id=f"HEAL_{len(recommendations):04d}",
                source=RecommendationSource.AUTO_HEALING,
                priority=RecommendationPriority.MEDIUM,
                title="Healing Recommendation",
                description=rec,
                impact="Improve job stability",
                implementation=rec,
                effort=ImplementationEffort.MODERATE,
                category="healing",
                created_at=datetime.utcnow().isoformat()
            ))

        return recommendations

    def _score_recommendations(self, recommendations: List[Recommendation]) -> List[Recommendation]:
        """Score and sort recommendations by value (priority/effort ratio)."""
        scored = []
        for rec in recommendations:
            priority_score = self.priority_weights.get(rec.priority, 50)
            effort_cost = self.effort_costs.get(rec.effort, 5)
            value_score = priority_score / effort_cost  # Higher is better
            rec.metadata["value_score"] = value_score
            scored.append(rec)

        # Sort by value score descending
        scored.sort(key=lambda r: r.metadata.get("value_score", 0), reverse=True)
        return scored

    def _count_by_priority(self, recommendations: List[Recommendation]) -> Dict[str, int]:
        """Count recommendations by priority."""
        counts = {}
        for rec in recommendations:
            key = rec.priority.value
            counts[key] = counts.get(key, 0) + 1
        return counts

    def _count_by_source(self, recommendations: List[Recommendation]) -> Dict[str, int]:
        """Count recommendations by source."""
        counts = {}
        for rec in recommendations:
            key = rec.source.value
            counts[key] = counts.get(key, 0) + 1
        return counts

    def _generate_implementation_order(self, recommendations: List[Recommendation]) -> List[str]:
        """Generate recommended order of implementation."""
        # Quick wins first (high priority + low effort)
        quick_wins = [r for r in recommendations
                     if r.priority in [RecommendationPriority.CRITICAL, RecommendationPriority.HIGH]
                     and r.effort in [ImplementationEffort.TRIVIAL, ImplementationEffort.EASY]]

        # Then remaining high priority
        high_priority = [r for r in recommendations
                        if r.priority in [RecommendationPriority.CRITICAL, RecommendationPriority.HIGH]
                        and r not in quick_wins]

        # Then medium priority
        medium_priority = [r for r in recommendations
                         if r.priority == RecommendationPriority.MEDIUM]

        # Combine in order
        ordered = quick_wins + high_priority + medium_priority
        return [r.id for r in ordered[:20]]  # Top 20

    def _estimate_total_effort(self, recommendations: List[Recommendation]) -> str:
        """Estimate total effort for all recommendations."""
        total_days = 0
        effort_to_days = {
            ImplementationEffort.TRIVIAL: 0.1,
            ImplementationEffort.EASY: 0.5,
            ImplementationEffort.MODERATE: 2,
            ImplementationEffort.HARD: 5,
            ImplementationEffort.COMPLEX: 10
        }

        for rec in recommendations:
            if rec.priority != RecommendationPriority.INFO:
                total_days += effort_to_days.get(rec.effort, 2)

        if total_days < 1:
            return "Less than 1 day"
        elif total_days < 5:
            return f"~{total_days:.0f} days"
        elif total_days < 20:
            return f"~{total_days/5:.0f} weeks"
        else:
            return f"~{total_days/20:.0f} months"

    def _estimate_total_benefit(self, recommendations: List[Recommendation]) -> str:
        """Estimate total benefit from implementing recommendations."""
        critical = sum(1 for r in recommendations if r.priority == RecommendationPriority.CRITICAL)
        high = sum(1 for r in recommendations if r.priority == RecommendationPriority.HIGH)
        medium = sum(1 for r in recommendations if r.priority == RecommendationPriority.MEDIUM)

        benefits = []
        if critical > 0:
            benefits.append(f"{critical} critical fixes")
        if high > 0:
            benefits.append(f"{high} high-impact improvements")
        if medium > 0:
            benefits.append(f"{medium} moderate improvements")

        return ", ".join(benefits) if benefits else "Minor improvements"

    def _map_severity_to_priority(self, severity: str) -> RecommendationPriority:
        """Map severity string to priority enum."""
        mapping = {
            "critical": RecommendationPriority.CRITICAL,
            "high": RecommendationPriority.HIGH,
            "medium": RecommendationPriority.MEDIUM,
            "low": RecommendationPriority.LOW,
            "info": RecommendationPriority.INFO
        }
        return mapping.get(severity.lower(), RecommendationPriority.MEDIUM)

    def _estimate_effort_from_code(self, rec: Dict) -> ImplementationEffort:
        """Estimate effort based on code recommendation type."""
        code = rec.get("suggested_code", "")
        if len(code) < 50:
            return ImplementationEffort.TRIVIAL
        elif len(code) < 200:
            return ImplementationEffort.EASY
        elif "# " in code and code.count("\n") > 10:
            return ImplementationEffort.MODERATE
        else:
            return ImplementationEffort.EASY

    def generate_report(self, plan: RecommendationPlan) -> str:
        """Generate markdown report of recommendations."""
        report = []
        report.append(f"# Recommendation Report: {plan.job_name}")
        report.append(f"\n**Generated:** {plan.timestamp}")
        report.append(f"**Total Recommendations:** {plan.total_recommendations}")

        # Summary
        report.append("\n## Summary")
        report.append("\n### By Priority")
        for priority, count in sorted(plan.by_priority.items()):
            report.append(f"- {priority.upper()}: {count}")

        report.append("\n### By Source")
        for source, count in sorted(plan.by_source.items()):
            report.append(f"- {source}: {count}")

        report.append(f"\n**Estimated Total Effort:** {plan.estimated_total_effort}")
        report.append(f"**Estimated Total Benefit:** {plan.estimated_total_benefit}")

        # Quick wins
        if plan.quick_wins:
            report.append("\n## Quick Wins (Start Here)")
            for rec in plan.quick_wins:
                report.append(f"\n### {rec.title}")
                report.append(f"- **Source:** {rec.source.value}")
                report.append(f"- **Effort:** {rec.effort.value}")
                report.append(f"- **Description:** {rec.description}")
                report.append(f"- **Implementation:** {rec.implementation}")
                if rec.code_snippet:
                    report.append("\n```python")
                    report.append(rec.code_snippet[:500])
                    report.append("```")

        # High impact
        if plan.high_impact:
            report.append("\n## High Impact Recommendations")
            for rec in plan.high_impact[:5]:
                if rec not in plan.quick_wins:
                    report.append(f"\n### {rec.title} ({rec.priority.value})")
                    report.append(f"- **Impact:** {rec.impact}")
                    report.append(f"- **Implementation:** {rec.implementation}")

        # Implementation order
        if plan.implementation_order:
            report.append("\n## Recommended Implementation Order")
            for i, rec_id in enumerate(plan.implementation_order[:10], 1):
                rec = next((r for r in plan.all_recommendations if r.id == rec_id), None)
                if rec:
                    report.append(f"{i}. [{rec.id}] {rec.title}")

        return "\n".join(report)

    def save_recommendations(self, plan: RecommendationPlan) -> None:
        """Save recommendations to DynamoDB for tracking."""
        if not self.dynamodb:
            return

        try:
            for rec in plan.all_recommendations:
                self.dynamodb.put_item(
                    TableName=self.recommendations_table,
                    Item={
                        "job_name": {"S": plan.job_name},
                        "recommendation_id": {"S": rec.id},
                        "source": {"S": rec.source.value},
                        "priority": {"S": rec.priority.value},
                        "title": {"S": rec.title},
                        "description": {"S": rec.description},
                        "status": {"S": rec.status.value},
                        "created_at": {"S": rec.created_at},
                        "effort": {"S": rec.effort.value},
                        "category": {"S": rec.category}
                    }
                )
        except Exception:
            pass
