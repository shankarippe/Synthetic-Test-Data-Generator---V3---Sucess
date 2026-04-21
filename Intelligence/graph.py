"""
intelligence/graph.py
---------------------
LangGraph workflow — wires all nodes into a directed graph.

Flow:
  schema_reader → domain_detector → column_inference →
  volume_inference → scenario_generator → config_writer →
  pipeline_executor → END
"""

from __future__ import annotations

from typing import Any
from langgraph.graph import StateGraph, END
from Intelligence.state import PipelineState
from Intelligence.nodes import (
    schema_reader_node,
    domain_detector_node,
    column_inference_node,
    volume_inference_node,
    scenario_generator_node,
    config_writer_node,
    pipeline_executor_node,
)


def build_graph(llm_client) -> Any:
    """
    Build and compile the LangGraph pipeline.
    Each node is a pure function: state_in → state_out.
    """

    # Wrap nodes to inject llm_client
    def schema_reader(state):     return schema_reader_node(state, llm_client)
    def domain_detector(state):   return domain_detector_node(state, llm_client)
    def column_inference(state):  return column_inference_node(state, llm_client)
    def volume_inference(state):  return volume_inference_node(state, llm_client)
    def scenario_generator(state):return scenario_generator_node(state, llm_client)
    def config_writer(state):     return config_writer_node(state, llm_client)
    def pipeline_executor(state): return pipeline_executor_node(state, llm_client)

    # Build graph
    workflow = StateGraph(PipelineState)

    # Add nodes
    workflow.add_node("schema_reader",      schema_reader)
    workflow.add_node("domain_detector",    domain_detector)
    workflow.add_node("column_inference",   column_inference)
    workflow.add_node("volume_inference",   volume_inference)
    workflow.add_node("scenario_generator", scenario_generator)
    workflow.add_node("config_writer",      config_writer)
    workflow.add_node("pipeline_executor",  pipeline_executor)

    # Wire edges (linear pipeline)
    workflow.set_entry_point("schema_reader")
    workflow.add_edge("schema_reader",      "domain_detector")
    workflow.add_edge("domain_detector",    "column_inference")
    workflow.add_edge("column_inference",   "volume_inference")
    workflow.add_edge("volume_inference",   "scenario_generator")
    workflow.add_edge("scenario_generator", "config_writer")
    workflow.add_edge("config_writer",      "pipeline_executor")
    workflow.add_edge("pipeline_executor",  END)

    return workflow.compile()