import { Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow } from "@material-ui/core"
import { divide } from "lodash"
import { useEffect, useState } from "react"
import View from "src/utils/View"

export default function StreamingDataTable(props) {
    const rows = useState([])

    useEffect(() => {
        connectSocket()
    }, [])

    function connectSocket() {

    }

    return <View>
        <Paper>
            <TableContainer >
                <Table size="small">
                    <TableHead>
                        <TableRow>
                            <TableCell>#</TableCell>
                            <TableCell>Asin</TableCell>
                            <TableCell>Overall</TableCell>
                            <TableCell>ReviewerId</TableCell>
                            <TableCell>ReviewText</TableCell>
                            <TableCell>Prediction</TableCell>
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        {rows.map((row, index) => <TableRow key={index}>
                            <TableCell>{index + 1}</TableCell>
                            <TableCell></TableCell>
                            <TableCell></TableCell>
                            <TableCell></TableCell>
                            <TableCell></TableCell>
                            <TableCell></TableCell>
                        </TableRow>)}
                    </TableBody>
                </Table>
            </TableContainer>
        </Paper>

    </View>
}