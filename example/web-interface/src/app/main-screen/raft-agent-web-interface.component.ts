/*! Copyright 2023 Siemens AG

   Licensed under the Apache License, Version 2.0 (the "License"); you may not
   use this file except in compliance with the License. You may obtain a copy of
   the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
   License for the specific language governing permissions and limitations under
   the License.
*/

import { Component, OnInit } from "@angular/core";
import { MatSnackBar } from "@angular/material/snack-bar";
import { Title } from "@angular/platform-browser";
import { ActivatedRoute } from "@angular/router";
import { Container } from "@coaty/core";
import { Observable } from "rxjs";

import { ConnectionState, RaftClusterConfiguration, RaftState } from "../../shared";
import { CoatyContainerService } from "../coaty-container.service";
import { RaftAgentController } from "../raft-agent-controller";

@Component({
    selector: "app-raft-agent-web-interface",
    templateUrl: "./raft-agent-web-interface.component.html",
    styleUrls: ["./raft-agent-web-interface.component.scss"],
})
export class RaftAgentWebInterfaceComponent implements OnInit {

    agentId = "";

    /** The connection state of the corresponding raft agent. */
    connectionState$!: Observable<ConnectionState>;
    connectionStateType = ConnectionState;

    /** The Raft state received from the corresponding raft agent. */
    raftState$!: Observable<RaftState>;

    /**
     * The Raft cluster configuration received from the corresponding raft
     * agent.
     */
    raftClusterConfiguration$!: Observable<RaftClusterConfiguration>;

    /** Set */
    setKey = "";
    setValue = "";
    setLoading = false;

    /** Delete */
    deleteKey = "";
    deleteLoading = false;

    /** Used to communicate with the corresponding raft agent. */
    private _controller!: RaftAgentController;
    private _container!: Container;

    constructor(
        private _titleService: Title,
        private _snackBar: MatSnackBar,
        private _route: ActivatedRoute,
        private _coatyContainerService: CoatyContainerService
    ) { }

    ngOnInit(): void {
        const id = this._route.snapshot.paramMap.get("id");
        if (id === null) {
            return;
        }
        this.agentId = id;
        this._titleService.setTitle("Raft Agent " + id);
        this._container = this._coatyContainerService.instantiateContainer(id);
        this._controller =
            this._container.getController<RaftAgentController>("RaftAgentController");
        this.connectionState$ = this._controller.connectionStateChange$;
        this.raftState$ = this._controller.raftStateChange$;
        this.raftClusterConfiguration$ = this._controller.raftClusterConfigurationChange$;
    }

    connect(): void {
        this._resetState();
        this._controller.connect().catch((errMsg) => {
            this._snackBar.open(errMsg, "close");
        });
    }

    set(key: string, value: string): void {
        this.setKey = "";
        this.setValue = "";
        this.setLoading = true;
        this._controller
            .set(key, value)
            .then(() => (this.setLoading = false))
            .catch((errMsg) => {
                this.setLoading = false;
                this._snackBar.open(errMsg, "close");
            });
    }

    delete(key: string): void {
        this.deleteKey = "";
        this.deleteLoading = true;
        this._controller
            .delete(key)
            .then(() => (this.deleteLoading = false))
            .catch((errMsg) => {
                this.deleteLoading = false;
                this._snackBar.open(errMsg, "close");
            });
    }

    stop(): void {
        this._controller
            .stop()
            .then(() => {
                this._resetState();
            })
            .catch((errMsg) => {
                this._snackBar.open(errMsg, "close");
            });
    }

    disconnect(): void {
        this._controller
            .disconnect()
            .then(() => {
                this._resetState();
            })
            .catch((errMsg) => {
                this._snackBar.open(errMsg, "close");
            });
    }

    private _resetState(): void {
        this.setKey = "";
        this.setValue = "";
        this.setLoading = false;
        this.deleteKey = "";
        this.deleteLoading = false;
    }
}
