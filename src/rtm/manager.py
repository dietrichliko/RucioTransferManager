"""Rucio Transfer Manager - manager.
"""
from doctest import debug
import logging
import asyncio
import json

from sqlalchemy.orm import Session
from sqlalchemy import Engine, create_engine, select

from rtm import model
from rtm import config

import rucio
import rucio.client


log = logging.getLogger(__name__)
cfg = config.get()


def human_readable_size(size, decimal_places=2):
    for unit in ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]:
        if size < 1024.0 or unit == "PiB":
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f} {unit}"


class Manager:
    engine: Engine
    sem_dasgoclient: asyncio.Semaphore
    sem_xrdadler32: asyncio.Semaphore
    client: rucio.client.Client

    def __init__(self) -> None:
        self.engine = create_engine(cfg.db_url, echo=cfg.db_echo)
        model.Base.metadata.create_all(self.engine)
        self.sem_dasgoclient = asyncio.Semaphore(cfg.max_dasgoclient)
        self.sem_xrdadler32 = asyncio.Semaphore(cfg.max_xrdadler32)
        self.client = rucio.client.Client(**cfg.rucio_client_args)

    def list_datasets(self, pattern: str) -> None:
        if pattern:
            stmt = select(model.Dataset).filter(model.Dataset.name.op("GLOB")(pattern))
        else:
            stmt = select(model.Dataset)
        total_size = 0
        total_files = 0
        with Session(self.engine) as session:
            for dataset in session.scalars(stmt):
                size = sum( f.size for f in dataset.files)
                nr_files = len(dataset.files)
                total_size += size
                total_files += nr_files
                sub = dataset.subscription if dataset.subscription else "-"
                print(
                    f"{human_readable_size(size):>11} {nr_files:6} "
                    f"[{dataset.ok_cnt:4}/{dataset.replicating_cnt:4}/{dataset.stuck_cnt:4}] "
                    f"{dataset.status[:4]:4} {dataset.site} {dataset.name} {sub}"
                )
        print(f"{human_readable_size(total_size):>11} {total_files:6} Total")

    def update(self, pattern: str) -> None:
        if pattern:
            log.debug("Updating status of %s", pattern)
        else:
            log.debug("Updating status")
        if pattern:
            stmt = select(model.Dataset).filter(
                model.Dataset.name.op("GLOB")(pattern),
                model.Dataset.status == "subscribed",
            )
        else:
            stmt = select(model.Dataset).filter(model.Dataset.status == "subscribed")
        with Session(self.engine) as session:
            for dataset in session.scalars(stmt):
                response = self.client.get_replication_rule(dataset.subscription)
                if response["state"] == "OK":
                    dataset.transfered()
                    session.commit()

    def verify(self, pattern: str) -> None:
        asyncio.run(self._verify(pattern))

    async def _verify(self, pattern: str) -> None:
        if pattern:
            stmt = select(model.Dataset).filter(
                model.Dataset.name.op("GLOB")(pattern),
                model.Dataset.status == "transferred",
            )
        else:
            stmt = select(model.Dataset).filter(model.Dataset.status == "transferred")
        with Session(self.engine) as session:
            dataset_ids = [ d.id for d in session.scalars(stmt)]
        for id in dataset_ids:
            await self._verify_dataset(id)


    async def _verify_dataset(self, id: int) -> None:
        tasks = []
        with Session(self.engine) as session:
            stmt = select(model.Dataset).where(model.Dataset.id == id)
            dataset = session.scalar(stmt)
            async with asyncio.TaskGroup() as tg:
                for file in dataset.files:
                    tasks.append(tg.create_task(self._verify_file(file.lfn, file.checksum)))
            if any(not t.result() for t in tasks):
                log.error("Dataset %s has bad files", dataset.name)
            else:
                log.info("Dataset %s bis ok", dataset.name)
                dataset.verified()
            session.commit()

    async def _verify_file(self, lfn: str, checksum: int) -> None:
        if lfn.index("Run2016") > 0:
            url = f"root://eospublic.cern.ch//eos/opendata/cms/{lfn[12:]}"
        else:
            url = f"root://eospublic.cern.ch//eos/opendata/cms/mc/{lfn[12:]}"

        log.debug("url: %s", url)
        async with self.sem_xrdadler32:
            proc = await asyncio.create_subprocess_exec(
                "xrdadler32", url, stdout=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            log.error("Return code %d from xrdadler32 %s, proc.returncode", url)
        checksum1 = int(stdout.decode().split()[0], 16)
        if checksum != checksum1:
            log.error("invalid checksum for %s", url)
        return checksum == checksum1

    def subscribe(self, pattern: str) -> None:
        if pattern:
            stmt = select(model.Dataset).filter(
                model.Dataset.name.op("GLOB")(pattern), model.Dataset.status == "new"
            )
        else:
            stmt = select(model.Dataset).filter(model.Dataset.status == "new")
        with Session(self.engine) as session:
            for dataset in session.scalars(stmt):
                dids = [{'scope': 'cms', 'name': dataset.name}]
                rse_expression = f"{dataset.site}"
                try:
                    response = self.client.add_replication_rule(
                        dids=dids,
                        copies=1,
                        rse_expression=rse_expression,
                    )
                except rucio.common.exception.RucioException as e:
                    log.exception("Error creating replication rule for %s", dataset.name)
                    dataset.error()
                    continue

                dataset.subscribe()
                dataset.subscription = response[0]
                log.info("Subscribed %s", dataset.name)
                session.commit()

    def add_datasets(self, datasets: list[str], site: str) -> None:
        asyncio.run(self._add_datasets(datasets, site))

    async def _add_datasets(self, datasets: list[str], site: str) -> None:
        with Session(self.engine) as session:
            async with asyncio.TaskGroup() as tg:
                for dataset in datasets:
                    tg.create_task(self._add_a_dataset(dataset, site, session))
            session.commit()

    async def _add_a_dataset(self, dataset: str, site: str, session: Session) -> None:
        obj = session.scalar(select(model.Dataset).filter_by(name=dataset))
        if obj is None:
            async with self.sem_dasgoclient:
                log.debug('Query for "%s"', dataset)
                proc = await asyncio.create_subprocess_exec(
                    "dasgoclient",
                    "-json",
                    f"-query=file dataset={dataset}",
                    stdout=asyncio.subprocess.PIPE,
                )
                stdout, stderr = await proc.communicate()
                dataset_info = json.loads(stdout)
                log.info("Creating %s", dataset)
                d_obj = model.Dataset(
                    name=dataset,
                    site=site,
                    ok_cnt=0,
                    replicating_cnt=0,
                    stuck_cnt=0,
                )
                session.add(d_obj)
                session.flush()
                rules = list(
                    self.client.list_replication_rules(
                        {
                            "scope": "cms",
                            "name": dataset,
                            "rse_expression": site,
                        }
                    )
                )
                if rules:
                    log.info("Already subscribed %s", dataset)
                    d_obj.subscription = rules[0]["id"]
                    d_obj.ok_cnt = rules[0]['locks_ok_cnt']
                    d_obj.replicating_cnt = rules[0]['locks_replicating_cnt']
                    d_obj.stuck_cnt = rules[0]['locks_stuck_cnt']
                    d_obj.subscribe()
                for info in dataset_info:
                    for f in info["file"]:
                        f_obj = model.File(
                            dataset_id=d_obj.id,
                            lfn=f["name"],
                            checksum=int(f["adler32"], 16),
                            size=int(f["size"]),
                            events=int(f["nevents"]),
                        )

                    session.add(f_obj)
                session.commit()

        else:
            log.debug("Dataset %s already defined", dataset)

    def whoami(self) -> None:
        """Show rucio identity."""
        response = self.client.whoami()
        log.info("Rucio account: %s", response["account"])