from typing import TYPE_CHECKING, Optional
from Node.LoadBalancer.Job import Job
from Node.LoadBalancer.LoadBalancer import LoadBalancer

if TYPE_CHECKING:
    from ML.modeltypes import MLModelType
    from Node.node import NodeTCPServer
    from Node.nodetypes import Member, MembershipList


class Scheduler:
    def __init__(self, node: "NodeTCPServer", load_balancer: "LoadBalancer"):
        self.jobs = []
        self.node = node
        self.load_balancer = load_balancer

    def schedule(self, job):
        self.jobs.append(job)

    def get_next_job(self) -> Optional[Job]:
        """Get the next job in the queue"""
        if len(self.jobs) == 0:
            return None
        return self.jobs[0]

    def pop_next_job(self) -> Optional[Job]:
        """Get the next job in the queue"""
        if len(self.jobs) == 0:
            return None
        return self.jobs.pop(0)

    def get_next_job_on(self, node: Member) -> Optional[Job]:
        """Get the next job for the node"""
        # get the next job for the node
        # if there are no jobs, return None
        for job in self.jobs:
            if job.node_scheduled_on == node:
                return job
        return None

    async def dispatch(self):
        """Dispatch a job to a node"""
        # get the next job, and send the job to the node
        # if there are no jobs, return
        job = self.pop_next_job()
        if job is None:
            return

        # get the best node for the job
        best_node = self.load_balancer.get_best_node(job)
        if best_node is None:
            # insert the job back into the queue
            self.schedule(job)
            return

        # send the job to the node
        results = self.node.broadcast_to(job.serialize(), best_node, recv=True)

    def schedule_on(self, node: Member, job: Job):
        """Schedule a job on a specific model"""
        job.node_scheduled_on = node
        self.schedule(job)
