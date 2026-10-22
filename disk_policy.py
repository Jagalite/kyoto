"""Pure physical-drive pressure policy shared by admission and deferral."""


def affected_disks(job, policy):
    pressured = set(policy.get('pressured_disks', []))
    devices = policy.get('job_disks', {}).get(job['id'])
    # Unknown mappings may touch any pressured device.
    return pressured if devices is None else pressured.intersection(devices)


def disk_admissible(job, running, policy):
    if not policy.get('pressure_aware', True):
        return True
    affected = affected_disks(job, policy)
    return not any(affected.intersection(affected_disks(other, policy)) for other in running)
